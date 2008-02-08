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

import os, re, sys, time, weakref, threading
from collections import deque
#from traceback import print_stack

_profile_git_calls = False

class GitError(Exception):
    pass

class GitErrorSha(GitError):
    pass

def git_version():
    try:
        (input, output, error) = os.popen3('git --version')
        [v] = output.readlines()
        [a,b,c] = v.strip().split()
        return c
    except:
        raise GitError

class StorageFactory:
    __dict = weakref.WeakValueDictionary()
    __dict_nonweak = dict()
    __dict_lock = threading.Lock()

    def __init__(self, repo, log, weak=True):
        self.logger = log

        StorageFactory.__dict_lock.acquire()

        try:
            i = StorageFactory.__dict[repo]
        except KeyError:
            i = Storage(repo, log)
            StorageFactory.__dict[repo] = i

        # create or remove additional reference depending on 'weak' argument
        if weak:
            try:
                del StorageFactory.__dict_nonweak[repo]
            except KeyError:
                pass
        else:
            StorageFactory.__dict_nonweak[repo] = i

        StorageFactory.__dict_lock.release()

        self.__inst = i
        self.__repo = repo

    def getInstance(self):
        is_weak = self.__repo not in StorageFactory.__dict_nonweak
        self.logger.debug("requested %sPyGIT.Storage instance %d for '%s'"
                          % (("","weak ")[is_weak], id(self.__inst), self.__repo))
        return self.__inst

class Storage:
    def __init__(self, repo, log):
        self.logger = log
        self.logger.debug("PyGIT.Storage instance %d constructed" % id(self))

        self.repo = repo
        self.commit_encoding = None

        self._lock = threading.Lock()
        self.last_youngest_rev = -1
        self._invalidate_caches()

    def __del__(self):
        self.logger.debug("PyGIT.Storage instance %d destructed" % id(self))

    def _invalidate_caches(self,youngest_rev=None):
        self._lock.acquire()

        rc = False

        if self.last_youngest_rev != youngest_rev:
            self.logger.debug("invalidated caches (%s != %s)" % (self.last_youngest_rev, youngest_rev))
            rc = True
            self._commit_db = None
            self._oldest_rev = None
            self.last_youngest_rev = None

        self._lock.release()
        return rc

    def get_commits(self):
        self._lock.acquire()
        if self._commit_db is None:
            self.logger.debug("triggered rebuild of commit tree db for %d" % id(self))
            new_db = {}
            new_tags = set([])
            parent = None
            youngest = None
            ord_rev = 0
            for revs in self._git_call_f("rev-parse", ["--tags"]).readlines():
                new_tags.add(revs.strip())

            for revs in self._git_call_f("rev-list", ["--parents", "--all"]).readlines():
                revs = revs.strip().split()

                rev = revs[0]
                parents = set(revs[1:])

                ord_rev += 1

                if not youngest:
                    youngest = rev

                # new_db[rev] = (children(rev), parents(rev), ordinal_id(rev))
                if new_db.has_key(rev):
                    _children,_parents,_ord_rev = new_db[rev]
                    assert _children
                    assert not _parents
                    assert _ord_rev == 0
                    new_db[rev] = (_children, parents, ord_rev)
                else:
                    new_db[rev] = (set(), parents, ord_rev)

                # update all parents(rev)'s children
                for parent in parents:
                    if new_db.has_key(parent):
                        new_db[parent][0].add(rev)
                    else:
                        new_db[parent] = (set([rev]), set(), 0) # dummy ordinal_id

            self._commit_db = new_db, parent, new_tags
            self.last_youngest_rev = youngest
            self.logger.debug("rebuilt commit tree db for %d with %d entries" % (id(self),len(new_db)))

        self._lock.release()

        assert self._commit_db[1] is not None
        assert self._commit_db[0] is not None

        return self._commit_db[0]

    def sync(self):
        rev = self._git_call("rev-list", ["--max-count=1", "--all"]).strip()
        return self._invalidate_caches(rev)

    def oldest_rev(self):
        self.get_commits() # trigger commit tree db build
        return self._commit_db[1]

    def youngest_rev(self):
        self.get_commits() # trigger commit tree db build
        return self.last_youngest_rev

    def history_relative_rev(self, sha, rel_pos):
        db = self.get_commits()

        if sha not in db:
            raise GitErrorSha

        if rel_pos == 0:
            return sha

        lin_rev = db[sha][2] + rel_pos

        if lin_rev < 1 or lin_rev > len(db):
            return None

        for k,v in db.iteritems():
            if v[2] == lin_rev:
                return k

        # should never be reached if db is consistent
        raise GitError

    def hist_next_revision(self, sha):
        return self.history_relative_rev(sha, -1)

    def hist_prev_revision(self, sha):
        return self.history_relative_rev(sha, +1)

    def _git_call_f(self, gitcmd, args=[]):
        #print "GIT: "+cmd
        if _profile_git_calls:
            t = time.time()
            pass

        cmd = ["git", '--git-dir=%s' % self.repo, gitcmd]
        cmd.extend(args)

        (input, output, error) = os.popen3(cmd)

        if _profile_git_calls:
            t = time.time() - t # doesn't work actually, as popen3 runs async
            print >>sys.stderr, "GIT: took %6.2fs for '%s'" % (t, cmd)
            pass

        return output

    def _git_call(self, cmd, args=[]):
        return self._git_call_f(cmd, args).read()

    def get_commit_encoding(self):
        if self.commit_encoding is None:
            self.commit_encoding = self._git_call("repo-config",
                                                  ["--get", "i18n.commitEncoding"]).strip()
            if ''==self.commit_encoding:
                self.commit_encoding = 'utf-8'
        return self.commit_encoding


    def head(self):
        "get current HEAD commit id"
        return self.verifyrev("HEAD")

    def verifyrev(self, rev):
        "verify/lookup given revision object and return a sha id or None if lookup failed"
        db = self.get_commits()
        tag_db = self._commit_db[2]

        rev = str(rev)

        if db.has_key(rev):
            return rev

        rc = self._git_call("rev-parse", ["--verify", rev]).strip()
        if len(rc)==0:
            return None

        if db.has_key(rc):
            return rc
        elif rc in tag_db:
            sha=self._git_call("cat-file", ["tag", rc]).split(None, 2)[:2]
            if sha[0] != 'object':
                self.logger.debug("unexpected result from 'git-cat-file tag %s'" % rc)
                return None
            return sha[1]

        return None

    def shortrev(self, rev):
        "try to shorten sha id"
        return self._git_call("rev-parse", ["--short", str(rev)]).strip()

    def get_branches(self):
        "returns list of branches, with active (= HEAD) one being the first item"
        result=[]
        for e in self._git_call_f("branch", ["-v", "--no-abbrev"]).readlines():
            (bname,bsha)=e[1:].strip().split()[:2]
            if e[0]=='*':
                result.insert(0,(bname,bsha))
            else:
                result.append((bname,bsha))
        return result

    def get_tags(self):
        result=[]
        for e in self._git_call_f("tag", ["-l"]).readlines():
            result.append(e.strip())
        return result

    def tree_ls(self, rev, path=""):
        rev = str(rev) # paranoia
        if path.startswith('/'):
            path = path[1:]
        return [e.split(None, 3) for e in self._git_call("ls-tree",
                                                         ["-z", rev, "--",
                                                          path]).split('\0') if e]

    def read_commit(self, sha):
        if not sha:
            raise GitErrorSha

        db = self.get_commits()
        if sha not in db:
            self.logger.info("read_commit failed for '%s'" % sha)
            raise GitErrorSha

        raw = self._git_call("cat-file", ["commit", str(sha)])
        raw = unicode(raw, self.get_commit_encoding(), 'replace')
        lines = raw.splitlines()

        if not lines:
            raise GitErrorSha

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
        return self._git_call_f("cat-file", ["blob", str(sha)])

    def get_obj_size(self, sha):
        return int(self._git_call("cat-file", ["-s", str(sha)]).strip())

    def children(self, sha):
        db = self.get_commits()

        try:
            return list(db[sha][0])
        except KeyError:
            return []

    def children_recursive(self, sha):
        db = self.get_commits()

        work_list = deque()
        seen = set()

        seen.update(db[sha][0])
        work_list.extend(db[sha][0])

        while work_list:
            p = work_list.popleft()
            yield p

            #_children = db[p][0]
            _children = db[p][0] - seen

            seen.update(_children)
            work_list.extend(_children)

        assert len(work_list) == 0

    def parents(self, sha):
        db = self.get_commits()

        try:
            return list(db[sha][1])
        except KeyError:
            return []

    def history(self, sha, path, limit=None, skip=0):
        if limit is None:
            limit = -1
        for rev in self._git_call_f("rev-list", 
                                    ["--max-count=%d" % limit, 
                                     str(sha), "--", path]).readlines():
            if(skip > 0):
                skip = skip - 1
                continue
            yield rev.strip()

    def all_revs(self):
        return self.get_commits().iterkeys()

    def history_timerange(self, start, stop):
        for rev in self._git_call_f("rev-list", ["--reverse",
                                                 "--max-age=%d" % start,
                                                 "--min-age=%d" % stop,
                                                 "--all"]).readlines():
            yield rev.strip()

    def rev_is_anchestor_of(self, rev1, rev2):
        """return True if rev2 is successor of rev1"""
        rev1 = rev1.strip()
        rev2 = rev2.strip()
        return rev2 in self.children_recursive(rev1)

    def blame(self, commit_sha, path):
        in_metadata = False
        args = ["-p", "--", path, str(commit_sha)]
        for line in self._git_call_f("blame", args).readlines():
            assert line
            if in_metadata:
                in_metadata = line[0] != '\t'
            else:
                split_line = line.split()
                if len(split_line) == 4:
                    (sha, orig_lineno, lineno, group_size) = split_line
                else:
                    (sha, orig_lineno, lineno) = split_line

                assert len(sha) == 40
                yield (sha, lineno)
                in_metadata = True

        assert not in_metadata

    def last_change(self, sha, path):
        last_rev = self._git_call("rev-list",
                                  ["--max-count=1", sha, "--", path])
        return last_rev.strip()

    def diff_tree(self, tree1, tree2, path=""):
        if tree1 is None:
            tree1 = "--root"
        for chg in self._git_call_f("diff-tree", ["-r",
                                                  str(tree1),
                                                  str(tree2),
                                                  "--", path]).readlines():
            if chg.startswith(tree2):
                continue
            (mode1,mode2,obj1,obj2,action,path) = chg[:-1].split(None, 5)
            mode1 = mode1[1:]
            yield (mode1,mode2,obj1,obj2,action,path)

if __name__ == '__main__':
    import sys, logging

    g = Storage(sys.argv[1], logging)

    print "[%s]" % g.head()
    print g.tree_ls(g.head())
    print "--------------"
    print g.read_commit(g.head())
    print "--------------"
    p = g.parents(g.head())
    print list(p)
    print "--------------"
    print list(g.children(list(p)[0]))
    print list(g.children(list(p)[0]))
    print "--------------"
    print g.get_commit_encoding()
    print "--------------"
    print g.get_branches()
    print "--------------"
    print g.hist_prev_revision(g.oldest_rev()), g.oldest_rev(), g.hist_next_revision(g.oldest_rev())

    print "--------------"
    p = g.youngest_rev()
    print g.hist_prev_revision(p), p, g.hist_next_revision(p)
    print "--------------"
    p = g.head()
    for i in range(-5,5):
        print i, g.history_relative_rev(p, i)

    # check for loops
    def check4loops(head):
        print "check4loops", head
        seen = set([head])
        for sha in g.children_recursive(head):
            if sha in seen:
                print "dupe detected :-/", sha, len(seen)
                #print seen
                #break
            seen.add(sha)
        return seen

    print len(check4loops(g.parents(g.head())[0]))

    #print len(check4loops(g.oldest_rev()))

    #print len(list(g.children_recursive(g.oldest_rev())))
