# -*- coding: iso-8859-1 -*-
#
# Copyright (C) 2006 Herbert Valerio Riedel <hvr@gnu.org>
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
from trac.util import TracError, shorten_line, escape
from trac.versioncontrol import Changeset, Node, Repository, \
                                IRepositoryConnector

import PyGIT

class GitConnector(Component):
	implements(IRepositoryConnector)

	def get_supported_types(self):
		yield ("git", 8)

	def get_repository(self, type, dir, authname):
		return GitRepository(dir, self.log)

class GitRepository(Repository):
	def __init__(self, path, log):
		self.gitrepo = path
		self.git = PyGIT.GIT(path)
		Repository.__init__(self, "git:"+path, None, log)

	def get_youngest_rev(self):
		return self.git.head()

	def normalize_path(self, path):
		return path and path.strip('/') or ''

	def normalize_rev(self, rev):
		if rev=='None' or rev == None or rev == '':
			return self.get_youngest_rev()
		return rev

	def get_oldest_rev(self):
		return ""

	def get_node(self, path, rev=None):
		#print "get_node", path, rev
		return GitNode(self.git, path, rev)

	def get_changeset(self, rev):
		#print "get_changeset", rev
		return GitChangeset(self.git, rev)

	def get_changes(self, old_path, old_rev, new_path, new_rev):
		if old_path != new_path:
			raise TracError("not supported in git_fs")
		#print "get_changes", (old_path, old_rev, new_path, new_rev)

		for chg in self.git.diff_tree(old_rev, new_rev, self.normalize_path(new_path)):
			#print chg
			(mode1,mode2,obj1,obj2,action,path) = chg
			kind = Node.FILE
			if mode2[0] == '1' or mode2[0] == '1':
				kind = Node.DIRECTORY
				
			if action == 'A':
				change = Changeset.ADD
			elif action == 'M':
				change = Changeset.EDIT
			elif action == 'D':
				change = Changeset.DELETE
			else:
				raise "OhOh"

			old_node = None
			new_node = None

			if change != Changeset.ADD:
				old_node = self.get_node(path, old_rev)
			if change != Changeset.DELETE:
				new_node = self.get_node(path, new_rev)

			yield (old_node, new_node, kind, change)

	def next_rev(self, rev, path=''):
		#print "next_rev"
		for c in self.git.children(rev):
			return c
		return None

	def previous_rev(self, rev):
		#print "previous_rev"
		for p in self.git.parents(rev):
			return p
		return None

	def rev_older_than(self, rev1, rev2):
		rc = rev1 in self.git.history(rev2, '', 1)
		#print "rev_older_than", (rev1, rev2, rc)
		return rc

	def sync(self):
		#print "GitRepository.sync"
		pass


class GitNode(Node):
	def __init__(self, git, path, rev):
		self.git = git
		self.sha = rev;
		self.perm = None;
		kind = Node.DIRECTORY
		p = path.strip('/')
		if p != "":
			[(self.perm,k,self.sha,fn)]=git.tree_ls(rev, p)
			rev=self.git.last_change(rev, p)
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

	def get_entries(self):
		if self.isfile:
			return
		if not self.isdir:
			return
		
		p = self.path.strip('/')
		if p != '': p = p + '/'
		for e in self.git.tree_ls(self.rev, p):
			yield GitNode(self.git, e[3], self.rev)
	
	def get_content_type(self):
		if self.isdir:
			return None
		return ''

	def get_content_length(self):
		if self.isfile:
			return len(self.get_content().read())
		return None

	def get_history(self, limit=None):
		p = self.path.strip('/')
		for rev in self.git.history(self.rev, p):
			yield (self.path, rev, Changeset.EDIT)


class GitChangeset(Changeset):
	def __init__(self, git, sha):
		self.git = git
		(msg,props) = git.read_commit(sha)
		self.props = props

		committer = props['committer'][0]
		(user,time,tz) = committer.rsplit(None, 2)
		
		Changeset.__init__(self, sha, msg, user, float(time))

	def get_properties(self):
		for k in self.props:
			v = self.props[k]
			if k in ['committer', 'author']:
				yield("git-"+k, ", ".join(v), False, 'author')
			if k in ['parent']:
				yield("git-"+k, ", ".join(("[%s]" % c) for c in v), True, 'changeset')

	def get_changes(self):
		#print "GitChangeset.get_changes"
		prev = self.props.has_key('parent') and self.props['parent'][0] or None
		for chg in self.git.diff_tree(prev, self.rev):
			#print chg
			(mode1,mode2,obj1,obj2,action,path) = chg
			kind = Node.FILE
			if mode1[0:1] == '04' or mode2[0:1] == '04':
				kind = Node.DIRECTORY
				
			if action == 'A':
				change = Changeset.ADD
			elif action == 'M':
				change = Changeset.EDIT
			elif action == 'D':
				change = Changeset.DELETE
			else:
				raise "OhOh"

			yield (path, kind, change, path, prev)
