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
from trac.util.datefmt import utc, FixedOffset
from trac.versioncontrol.api import \
    Changeset, Node, Repository, IRepositoryConnector, NoSuchChangeset, NoSuchNode
from trac.wiki import IWikiSyntaxProvider
from trac.versioncontrol.cache import CachedRepository
from trac.versioncontrol.web_ui import IPropertyRenderer
from trac.config import _TRUE_VALUES as TRUE

from genshi.builder import tag
from genshi.core import Markup, escape

from datetime import datetime
import time

import pkg_resources
pkg_resources.require('Trac>=0.11dev')

import PyGIT

class GitConnector(Component):
	implements(IRepositoryConnector, IWikiSyntaxProvider, IPropertyRenderer)

	def __init__(self):
		self._version = None

		try:
			self._version = PyGIT.git_version()
		except PyGIT.GitError, e:
			self.log.error("GitError: "+e.message)

		if self._version:
			self.log.info("detected GIT version %s" % self._version['v_str'])
			self.env.systeminfo.append(('GIT', self._version['v_str']))
			if self._version['v_compatible']:
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

	_rendered_props = ('Parents','Children','git-committer','git-author')

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

	def get_supported_types(self):
		yield ("git", 8)

	def get_repository(self, type, dir, authname):
		"""GitRepository factory method"""
		if not self._version:
			raise TracError("GIT backend not available")
		elif not self._version['v_compatible']:
			raise TracError("GIT version %s installed not compatible (need >= %s)" %
					(self._version['v_str'], self._version['v_min_str']))

		options = dict(self.config.options(type))

		repos = GitRepository(dir, self.log, options)

		cached_repository = 'cached_repository' in options and options['cached_repository'] in TRUE

		if cached_repository:
			repos = CachedRepository(self.env.get_db_cnx(), repos, None, self.log)
			self.log.info("enabled CachedRepository for '%s'" % dir)
		else:
			self.log.info("disabled CachedRepository for '%s'" % dir)

		return repos

class GitRepository(Repository):
	def __init__(self, path, log, options):
		self.logger = log
		self.gitrepo = path

		persistent_cache = 'persistent_cache' in options and options['persistent_cache'] in TRUE

		self.git = PyGIT.StorageFactory(path, log, not persistent_cache).getInstance()
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
		if rev=='None' or rev == None or rev == '':
			return self.get_youngest_rev()
		normrev=self.git.verifyrev(rev)
		if normrev is None:
			raise NoSuchChangeset(rev)
		return normrev

	def short_rev(self, rev):
		return self.git.shortrev(self.normalize_rev(rev))

	def get_node(self, path, rev=None):
		return GitNode(self.git, path, rev)

	def get_quickjump_entries(self, rev):
		for bname,bsha in self.git.get_branches():
			yield 'branches', bname, '/', bsha
		for t in self.git.get_tags():
			yield 'tags', t, '/', t

	def get_changesets(self, start, stop):
		#print "get_changesets", start, stop
		def to_unix(dt):
			return time.mktime(dt.timetuple()) + dt.microsecond/1e6

		for rev in self.git.history_timerange(to_unix(start), to_unix(stop)):
			yield self.get_changeset(rev)

	def get_changeset(self, rev):
		"""GitChangeset factory method"""
		return GitChangeset(self.git, rev)

	def get_changes(self, old_path, old_rev, new_path, new_rev):
		if old_path != new_path:
			raise TracError("not supported in git_fs")
		#print "get_changes", (old_path, old_rev, new_path, new_rev)

		for chg in self.git.diff_tree(old_rev, new_rev, self.normalize_path(new_path)):
			(mode1,mode2,obj1,obj2,action,path) = chg

			if mode2[0] == '1' or mode2[0] == '1':
				kind = Node.DIRECTORY
			else:
				kind = Node.FILE

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

	def previous_rev(self, rev):
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
			for r in revs:
				rev_callback(r)

class GitNode(Node):
	def __init__(self, git, path, rev, ls_tree_info=None):
		self.git = git
		self.sha = None
		self.perm = None
		self.data_len = None

		kind = Node.DIRECTORY
		p = path.strip('/')
		if p != "":
                        if ls_tree_info == None or ls_tree_info == "":
				ls_tree_info = git.ls_tree(rev, p)
                                if ls_tree_info != []:
                                        [ls_tree_info] = ls_tree_info
                                else:
                                        ls_tree_info = None

			if ls_tree_info != None:
				(self.perm, k, self.sha, fn) = ls_tree_info
                        else:
                                k = 'blob'

			rev = self.git.last_change(rev, p) # FIXME

			if k=='tree':
				pass
			elif k=='blob':
				kind = Node.FILE
			else:
				self.log.debug("kind is "+k)

		Node.__init__(self, path, rev, kind)

		self.created_path = path
		self.created_rev = rev

	def get_content(self):
		#print "get_content ", self.path, self.sha
		if self.isfile:
			return self.git.get_file(self.sha)

		return None

	def get_properties(self):
		if self.perm:
			return {'mode': self.perm }
		return {}

	def get_annotations(self):
		p = self.path.strip('/')
		if not self.isfile:
			return

		result = []
		for brev,blineno in self.git.blame(self.rev, p):
			result.append(brev)

		return result

	def get_entries(self):
		if not self.isdir:
			return

		p = self.path.strip('/')
		if p != '': p = p + '/'
		for e in self.git.ls_tree(self.rev, p):
			yield GitNode(self.git, e[3], self.rev, e)

	def get_content_type(self):
		if self.isdir:
			return None
		return ''

	def get_content_length(self):
		if self.isfile:
			if not self.data_len:
				self.data_len = self.git.get_obj_size(self.sha)
			return self.data_len
		return None

	def get_history(self, limit=None):
		#print "get_history", limit, self.path
		p = self.path.strip('/')
		for rev in self.git.history(self.rev, p, limit):
			yield (self.path, rev, Changeset.EDIT)

	def get_last_modified(self):
		return None

class GitChangeset(Changeset):

	action_map = {
		'A': Changeset.ADD,
		'M': Changeset.EDIT,
		'D': Changeset.DELETE 
		}

	# helper
	def __parse_user_time(self, s):
		"""parse author/committer attribute lines and return
		(user,timestamp)"""
		(user,time,tz_str) = s.rsplit(None, 2)
		tz = FixedOffset((int(tz_str)*6)/10, tz_str)
		time = datetime.fromtimestamp(float(time), tz)
		return (user,time)

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
		(user_, time_) = self.__parse_user_time(props['committer'][0])

		Changeset.__init__(self, sha, msg, user_, time_)

	def get_properties(self):
		properties = {}
		if 'parent' in self.props:
			properties['Parents'] = self.props['parent']
		if 'children' in self.props:
			properties['Children'] = self.props['children']
		if 'committer' in self.props:
			properties['git-committer'] = \
			    self.__parse_user_time(self.props['committer'][0])
		if 'author' in self.props:
			git_author = self.__parse_user_time(self.props['author'][0])
			if not (properties.has_key('git-committer') and
				properties['git-committer'] == git_author):
				properties['git-author'] = git_author

		return properties

	def get_changes(self):
		#print "GitChangeset.get_changes"
		prev = self.props.has_key('parent') and self.props['parent'][0] or None
		for chg in self.git.diff_tree(prev, self.rev):
			(mode1,mode2,obj1,obj2,action,path) = chg
			kind = Node.FILE
			if mode1[0:1] == '04' or mode2[0:1] == '04':
				kind = Node.DIRECTORY

			change = GitChangeset.action_map[action]

			yield (path, kind, change, path, prev)
