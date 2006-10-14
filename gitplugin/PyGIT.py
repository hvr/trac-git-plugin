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

import os, re

class GitError(Exception):
    pass

class Storage:
    def __init__(self,repo):
        self.repo = repo
        self.commit_encoding = None

    def _git_call_f(self,cmd):
        #print "GIT: "+cmd
        (input, output, error) = os.popen3('GIT_DIR="%s" %s' % (self.repo,cmd))
        return output

    def _git_call(self,cmd):
        return self._git_call_f(cmd).read()

    def get_commit_encoding(self):
        if self.commit_encoding is None:
            self.commit_encoding = self._git_call("git-repo-config --get i18n.commitEncoding").strip()
            if ''==self.commit_encoding:
                self.commit_encoding = 'utf-8'
        return self.commit_encoding

    def head(self):
        return self._git_call("git-rev-parse --verify HEAD").strip()

    def tree_ls(self,sha,path=""):
        if len(path)>0 and path[0]=='/':
            path=path[1:]
        return [e[:-1].split(None, 3) for e in self._git_call_f("git-ls-tree %s '%s'" % (sha,path)).readlines()]

    def read_commit(self, sha):
        raw = self._git_call("git-cat-file commit "+sha)
        raw = unicode(raw, self.get_commit_encoding(), 'replace')
        lines = raw.splitlines()

        line = lines.pop(0)
        d = {}
        while line != "":
            (key,value)=line.split(None, 1)
            if not d.has_key(key):
                d[key] = []
            d[key].append(value.strip())
            line = lines.pop(0)

        return ("\n".join(lines),d)

    def get_file(self, sha):
        return self._git_call_f("git-cat-file blob "+sha)

    def get_obj_size(self, sha):
        return int(self._git_call("git-cat-file -s "+sha).strip())

    def parents(self, sha):
        tmp=self._git_call("git-rev-list --max-count=1 --parents "+sha)
        tmp=tmp.strip()
        tmp=tmp.split()
        return tmp[1:]

    def children(self, sha):
        for revs in self._git_call_f("git-rev-list --parents HEAD").readlines():
            revs = revs.strip()
            revs = revs.split()
            if sha in revs[1:]:
                yield revs[0]
        
    def history(self, sha, path, skip=0):
        for rev in self._git_call_f("git-rev-list %s -- '%s'" % (sha,path)).readlines():
            if(skip > 0):
                skip = skip - 1
                continue
            yield rev.strip()

    def last_change(self, sha, path):
        for rev in self._git_call_f("git-rev-list --max-count=1 %s -- '%s'" % (sha,path)).readlines():
            return rev.strip()
        return None

    def diff_tree(self, tree1, tree2, path=""):
        if tree1 is None:
            tree1 = "--root"
        cmd = "git-diff-tree -r %s %s -- '%s'" % (tree1, tree2, path)
        for chg in self._git_call_f(cmd).readlines():
            if chg.startswith(tree2):
                continue
            (mode1,mode2,obj1,obj2,action,path) = chg[:-1].split(None, 5)
            mode1 = mode1[1:]
            yield (mode1,mode2,obj1,obj2,action,path)

if __name__ == '__main__':
    import sys
    
    g = Storage(sys.argv[1])
    
    print "[%s]" % g.head()
    print g.tree_ls(g.head())
    print "--------------"
    print g.read_commit(g.head())
    print "--------------"
    print g.parents(g.head())

    print "--------------"
    print g.get_commit_encoding()
