# -*- coding: iso-8859-1 -*-
#
# Copyright (C) 2006,2008 Herbert Valerio Riedel <hvr@gnu.org>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

from trac.core import *
from trac.util import TracError, shorten_line
from trac.util.datefmt import FixedOffset, to_timestamp
from trac.versioncontrol.api import \
    Changeset, Node, Repository, IRepositoryConnector, NoSuchChangeset, NoSuchNode
from trac.wiki import IWikiSyntaxProvider
from trac.versioncontrol.cache import CachedRepository
from trac.versioncontrol.web_ui import IPropertyRenderer
from trac.config import BoolOption, IntOption, PathOption, Option

# for some reason CachedRepository doesn't pass-through short_rev()s
class CachedRepository2(CachedRepository):
	def short_rev(self, path):
		return self.repos.short_rev(path)

from genshi.builder import tag
from genshi.core import Markup, escape

from datetime import datetime
import time, sys

if not sys.version_info[:2] >= (2,5):
	raise TracError("python >= 2.5 dependancy not met")

import PyGIT

def _last_iterable(iterable):
	"helper for detecting last iteration in for-loop"
        i = iter(iterable)
        v = i.next()
        for nextv in i:
		yield False, v
		v = nextv
	yield True, v

# helper
def _parse_user_time(s):
	"""parse author/committer attribute lines and return
	(user,timestamp)"""
	(user,time,tz_str) = s.rsplit(None, 2)
	tz = FixedOffset((int(tz_str)*6)/10, tz_str)
	time = datetime.fromtimestamp(float(time), tz)
	return (user,time)

class GitConnector(Component):
	implements(IRepositoryConnector, IWikiSyntaxProvider, IPropertyRenderer)

	def __init__(self):
		self._version = None

		try:
			self._version = PyGIT.Storage.git_version(git_bin=self._git_bin)
		except PyGIT.GitError, e:
			self.log.error("GitError: "+e.message)

		if self._version:
			self.log.info("detected GIT version %s" % self._version['v_str'])
			self.env.systeminfo.append(('GIT', self._version['v_str']))
			if not self._version['v_compatible']:
				self.log.error("GIT version %s installed not compatible (need >= %s)" %
					       (self._version['v_str'], self._version['v_min_str']))

	def _format_sha_link(self, formatter, ns, sha, label, fullmatch=None):
		try:
			changeset = self.env.get_repository().get_changeset(sha)
			return tag.a(label, class_="changeset",
				     title=shorten_line(changeset.message),
				     href=formatter.href.changeset(sha))
		except TracError, e:
			return tag.a(label, class_="missing changeset",
				     href=formatter.href.changeset(sha),
				     title=unicode(e), rel="nofollow")

	#######################
	# IPropertyRenderer

	# relied upon by GitChangeset

        def match_property(self, name, mode):
		if name in ('Parents','Children','git-committer','git-author') \
			    and mode == 'revprop':
			return 8 # default renderer has priority 1
		return 0

        def render_property(self, name, mode, context, props):
		def sha_link(sha):
			return self._format_sha_link(context, 'sha', sha, sha)

		if name in ('Parents','Children'):
			revs = props[name]

			return tag([tag(sha_link(rev), ', ') for rev in revs[:-1]],
				   sha_link(revs[-1]))

		if name in ('git-committer', 'git-author'):
			user_,time_ = props[name]
			_str = user_ + " / " + time_.strftime('%Y-%m-%dT%H:%M:%SZ%z')
			return unicode(_str)

		raise TracError("internal error")

	#######################
	# IWikiSyntaxProvider

	def get_wiki_syntax(self):
		yield (r'(?:\b|!)[0-9a-fA-F]{40,40}\b',
		       lambda fmt, sha, match:
			       self._format_sha_link(fmt, 'changeset', sha, sha))

	def get_link_resolvers(self):
		yield ('sha', self._format_sha_link)

	#######################
	# IRepositoryConnector

	_persistent_cache = BoolOption('git', 'persistent_cache', 'false',
				       "enable persistent caching of commit tree")

	_cached_repository = BoolOption('git', 'cached_repository', 'false',
					"wrap `GitRepository` in `CachedRepository`")

	_shortrev_len = IntOption('git', 'shortrev_len', 7,
				  "length rev sha sums should be tried to be abbreviated to"
				  " (must be >= 4 and <= 40)")

	_git_bin = PathOption('git', 'git_bin', '/usr/bin/git', "path to git executable (relative to trac project folder!)")


	def get_supported_types(self):
		yield ("git", 8)

	def get_repository(self, type, dir, authname):
		"""GitRepository factory method"""
		assert type == "git"

		if not self._version:
			raise TracError("GIT backend not available")
		elif not self._version['v_compatible']:
			raise TracError("GIT version %s installed not compatible (need >= %s)" %
					(self._version['v_str'], self._version['v_min_str']))

		repos = GitRepository(dir, self.log,
				      persistent_cache=self._persistent_cache,
				      git_bin=self._git_bin,
				      shortrev_len=self._shortrev_len)

		if self._cached_repository:
			repos = CachedRepository2(self.env.get_db_cnx(), repos, None, self.log)
			self.log.info("enabled CachedRepository for '%s'" % dir)
		else:
			self.log.info("disabled CachedRepository for '%s'" % dir)

		return repos

class GitRepository(Repository):
	def __init__(self, path, log, persistent_cache=False, git_bin='git', shortrev_len=7):
		self.logger = log
		self.gitrepo = path
		self._shortrev_len = max(4, min(shortrev_len, 40))

		self.git = PyGIT.StorageFactory(path, log, not persistent_cache,
						git_bin=git_bin).getInstance()
		Repository.__init__(self, "git:"+path, None, log)

	def close(self):
		self.git = None

	def clear(self, youngest_rev=None):
 	        self.youngest = None
		if youngest_rev is not None:
			self.youngest = self.normalize_rev(youngest_rev)
		self.oldest = None

	def get_youngest_rev(self):
		return self.git.youngest_rev()

	def get_oldest_rev(self):
		return self.git.oldest_rev()

	def normalize_path(self, path):
		return path and path.strip('/') or ''

	def normalize_rev(self, rev):
		if not rev:
			return self.get_youngest_rev()
		normrev=self.git.verifyrev(rev)
		if normrev is None:
			raise NoSuchChangeset(rev)
		return normrev

	def short_rev(self, rev):
		return self.git.shortrev(self.normalize_rev(rev), min_len=self._shortrev_len)

	def get_node(self, path, rev=None):
		return GitNode(self.git, path, rev, self.log)

	def get_quickjump_entries(self, rev):
		for bname,bsha in self.git.get_branches():
			yield 'branches', bname, '/', bsha
		for t in self.git.get_tags():
			yield 'tags', t, '/', t

	def get_changesets(self, start, stop):
		for rev in self.git.history_timerange(to_timestamp(start), to_timestamp(stop)):
			yield self.get_changeset(rev)

	def get_changeset(self, rev):
		"""GitChangeset factory method"""
		return GitChangeset(self.git, rev)

	def get_changes(self, old_path, old_rev, new_path, new_rev, ignore_ancestry=0):
		# TODO: handle renames/copies, ignore_ancestry
		if old_path != new_path:
			raise TracError("not supported in git_fs")

		for chg in self.git.diff_tree(old_rev, new_rev, self.normalize_path(new_path)):
			(mode1,mode2,obj1,obj2,action,path,path2) = chg

			kind = Node.FILE
			if mode2.startswith('04') or mode1.startswith('04'):
				kind = Node.DIRECTORY

			change = GitChangeset.action_map[action]

			old_node = None
			new_node = None

			if change != Changeset.ADD:
				old_node = self.get_node(path, old_rev)
			if change != Changeset.DELETE:
				new_node = self.get_node(path, new_rev)

			yield (old_node, new_node, kind, change)

	def next_rev(self, rev, path=''):
		return self.git.hist_next_revision(rev)

	def previous_rev(self, rev, path=''):
		return self.git.hist_prev_revision(rev)

	def rev_older_than(self, rev1, rev2):
		rc = self.git.rev_is_anchestor_of(rev1, rev2)
		return rc

	def clear(self, youngest_rev=None):
		self.sync()

	def sync(self, rev_callback=None):
		if rev_callback:
			revs = set(self.git.all_revs())

		if not self.git.sync():
			return None # nothing expected to change

		if rev_callback:
			revs = set(self.git.all_revs()) - revs
			for rev in revs:
				rev_callback(rev)

class GitNode(Node):
	def __init__(self, git, path, rev, log, ls_tree_info=None):
		self.log = log
		self.git = git
		self.fs_sha = None # points to either tree or blobs
		self.fs_perm = None
		self.fs_size = None

		kind = Node.DIRECTORY
		p = path.strip('/')
		if p: # ie. not the root-tree
                        if not ls_tree_info:
				ls_tree_info = git.ls_tree(rev, p) or None
                                if ls_tree_info:
                                        [ls_tree_info] = ls_tree_info

			if not ls_tree_info:
				raise NoSuchNode(path, rev)

			(self.fs_perm, k, self.fs_sha, self.fs_size, fn) = ls_tree_info

			# fix-up to the last commit-rev that touched this node
			rev = self.git.last_change(rev, p)

			if k=='tree':
				pass
			elif k=='blob':
				kind = Node.FILE
			else:
				raise TracError("internal error (got unexpected object kind '%s')" % k)

		self.created_path = path
		self.created_rev = rev

		Node.__init__(self, path, rev, kind)

	def __git_path(self):
		"return path as expected by PyGIT"
		p = self.path.strip('/')
		if self.isfile:
			assert p
			return p
		if self.isdir:
			return p and (p + '/')

		raise TracError("internal error")

	def get_content(self):
		if not self.isfile:
			return None

		return self.git.get_file(self.fs_sha)

	def get_properties(self):
		return self.fs_perm and {'mode': self.fs_perm } or {}

	def get_annotations(self):
		if not self.isfile:
			return

		return [ rev for (rev,lineno) in self.git.blame(self.rev, self.__git_path()) ]

	def get_entries(self):
		if not self.isdir:
			return

		for ent in self.git.ls_tree(self.rev, self.__git_path()):
			yield GitNode(self.git, ent[-1], self.rev, self.log, ent)

	def get_content_type(self):
		if self.isdir:
			return None

		return ''

	def get_content_length(self):
		if not self.isfile:
			return None

		if self.fs_size is None:
			self.fs_size = self.git.get_obj_size(self.fs_sha)

		return self.fs_size

	def get_history(self, limit=None):
		# TODO: find a way to follow renames/copies
		for is_last,rev in _last_iterable(self.git.history(self.rev, self.__git_path(), limit)):
			yield (self.path, rev, Changeset.EDIT if not is_last else Changeset.ADD)

	def get_last_modified(self):
		if not self.isfile:
			return None

		try:
			msg, props = self.git.read_commit(self.rev)
			user,ts = _parse_user_time(props['committer'][0])
		except:
			self.log.error("internal error (could not get timestamp from commit '%s')" % self.rev)
			return None

		return ts

class GitChangeset(Changeset):

	action_map = { # see also git-diff-tree(1) --diff-filter
		'A': Changeset.ADD,
		'M': Changeset.EDIT, # modified
		'T': Changeset.EDIT, # file type (mode) change
		'D': Changeset.DELETE,
		'R': Changeset.MOVE, # renamed
		'C': Changeset.COPY
		} # TODO: U, X, B

	def __init__(self, git, sha):
		self.git = git
		try:
			(msg, props) = git.read_commit(sha)
		except PyGIT.GitErrorSha:
			raise NoSuchChangeset(sha)
		self.props = props

		assert 'children' not in props
		_children = list(git.children(sha))
		if _children:
			props['children'] = _children

		# use 1st committer as changeset owner/timestamp
		(user_, time_) = _parse_user_time(props['committer'][0])

		Changeset.__init__(self, sha, msg, user_, time_)

	def get_properties(self):
		properties = {}
		if 'parent' in self.props:
			properties['Parents'] = self.props['parent']
		if 'children' in self.props:
			properties['Children'] = self.props['children']
		if 'committer' in self.props:
			properties['git-committer'] = \
			    _parse_user_time(self.props['committer'][0])
		if 'author' in self.props:
			git_author = _parse_user_time(self.props['author'][0])
			if not properties.get('git-committer') == git_author:
				properties['git-author'] = git_author

		return properties

	def get_changes(self):
		paths_seen = set()
		for parent in self.props.get('parent', [None]):
			for mode1,mode2,obj1,obj2,action,path1,path2 in \
				    self.git.diff_tree(parent, self.rev, find_renames=True):
				path = path2 or path1
				p_path, p_rev = path1, parent

				kind = Node.FILE
				if mode2.startswith('04') or mode1.startswith('04'):
					kind = Node.DIRECTORY

				action = GitChangeset.action_map[action[0]]

				if action == Changeset.ADD:
					p_path = ''
					p_rev = None

				# CachedRepository expects unique (rev, path, change_type) key
				# this is only an issue in case of merges where files required editing
				if path in paths_seen:
					continue

				paths_seen.add(path)

				yield (path, kind, action, p_path, p_rev)
