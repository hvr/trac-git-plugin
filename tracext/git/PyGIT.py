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

from __future__ import with_statement

import os, re, sys, time, weakref
from collections import deque
from functools import partial
from threading import Lock
#from traceback import print_stack

__all__ = ["git_version", "GitError", "GitErrorSha", "Storage", "StorageFactory"]

class GitError(Exception):
    pass

class GitErrorSha(GitError):
    pass

class GitCore:
    def __init__(self, git_dir=None, git_bin="git"):
        self.__git_bin = git_bin
        self.__git_dir = git_dir

    def __execute2(self, gitcmd, *args):
        # construct command tuple
        cmd = [self.__git_bin]
        if self.__git_dir:
            cmd.append('--git-dir=%s' % self.__git_dir)
        cmd.append(gitcmd)
        cmd.extend(args)

        #print >>sys.stderr, "GitCore '%s'" % str(cmd)
        return os.popen3(cmd) # (input, output, error)

    def __execute(self, git_cmd, *cmd_args):
        return self.__execute2(git_cmd, *cmd_args)[1]

    def __getattr__(self, name):
        return partial(self.__execute, name.replace('_','-'))


# helper class for caching...
class SizedDict(dict):
    def __init__(self, max_size=0):
        dict.__init__(self)
        self.__max_size = max_size
        self.__key_fifo = deque()
        self.__lock = Lock()

    def __setitem__(self, name, value):
        with self.__lock:
            assert len(self) == len(self.__key_fifo) # invariant

            if not self.__contains__(name):
                self.__key_fifo.append(name)

            rc = dict.__setitem__(self, name, value)

            while len(self.__key_fifo) > self.__max_size:
                self.__delitem__(self.__key_fifo.popleft())

            assert len(self) == len(self.__key_fifo) # invariant

            return rc

    def setdefault(k,d=None):
        # TODO
        raise AttributeError("SizedDict has no setdefault() method")

class StorageFactory:
    __dict = weakref.WeakValueDictionary()
    __dict_nonweak = dict()
    __dict_lock = Lock()

    def __init__(self, repo, log, weak=True):
        self.logger = log

        with StorageFactory.__dict_lock:
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

        self.__inst = i
        self.__repo = repo

    def getInstance(self):
        is_weak = self.__repo not in StorageFactory.__dict_nonweak
        self.logger.debug("requested %sPyGIT.Storage instance %d for '%s'"
                          % (("","weak ")[is_weak], id(self.__inst), self.__repo))
        return self.__inst

class Storage:
    __SREV_MIN = 6 # minimum short-rev length

    @staticmethod
    def git_version():
        GIT_VERSION_MIN_REQUIRED = (1,5,2)
        try:
            g = GitCore()
            output = g.version()
            [v] = output.readlines()
            [a,b,version] = v.strip().split()
            split_version = tuple(map(int, version.split('.')))

            result = {}
            result['v_str'] = version
            result['v_tuple'] = split_version
            result['v_min_tuple'] = GIT_VERSION_MIN_REQUIRED
            result['v_min_str'] = ".".join(map(str, GIT_VERSION_MIN_REQUIRED))
            result['v_compatible'] = split_version >= GIT_VERSION_MIN_REQUIRED
            return result
        except:
            raise GitError("Could not retrieve GIT version")

    def __init__(self, git_dir, log):
        self.logger = log
        self.logger.debug("PyGIT.Storage instance %d constructed" % id(self))

        self.git_dir = git_dir
        self.repo = GitCore(git_dir)

        self.commit_encoding = None

        self._lock = Lock()
        self.last_youngest_rev = -1
        self._invalidate_caches()

        # cache the last 200 commit messages
        self.__commit_msg_cache = SizedDict(200)
        self.__commit_msg_lock = Lock()

        # cache the last 2000 file sizes
        self.__fs_obj_size_cache = SizedDict(2000)
        self.__fs_obj_size_lock = Lock()

    def __del__(self):
        self.logger.debug("PyGIT.Storage instance %d destructed" % id(self))

    def _invalidate_caches(self,youngest_rev=None):
        with self._lock:
            rc = False
            if self.last_youngest_rev != youngest_rev:
                self.logger.debug("invalidated caches (%s != %s)" % (self.last_youngest_rev, youngest_rev))
                rc = True
                self._commit_db = None
                self._oldest_rev = None
                self.last_youngest_rev = None

            return rc

    def get_commits(self):
        with self._lock:
            if self._commit_db is None:
                self.logger.debug("triggered rebuild of commit tree db for %d" % id(self))
                new_db = {}
                new_sdb = {}
                new_tags = set([])
                parent = None
                youngest = None
                ord_rev = 0
                for revs in self.repo.rev_parse("--tags").readlines():
                    new_tags.add(revs.strip())

                for revs in self.repo.rev_list("--parents", "--all").readlines():
                    revs = revs.strip().split()

                    rev = revs[0]

                    # shortrev "hash" map
                    new_sdb.setdefault(rev[:self.__SREV_MIN], set()).add(rev)

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

                self._commit_db = new_db, parent, new_tags, new_sdb
                self.last_youngest_rev = youngest
                self.logger.debug("rebuilt commit tree db for %d with %d entries" % (id(self),len(new_db)))

            assert self._commit_db[1] is not None
            assert self._commit_db[0] is not None

            return self._commit_db[0]

    def sync(self):
        rev = self.repo.rev_list("--max-count=1", "--all").read().strip()
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
        raise GitError("internal inconsistency detected")

    def hist_next_revision(self, sha):
        return self.history_relative_rev(sha, -1)

    def hist_prev_revision(self, sha):
        return self.history_relative_rev(sha, +1)

    def get_commit_encoding(self):
        if self.commit_encoding is None:
            self.commit_encoding = \
                self.repo.repo_config("--get", "i18n.commitEncoding").read().strip() or 'utf-8'

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

        rc = self.repo.rev_parse("--verify", rev).read().strip()
        if not rc:
            return None

        if db.has_key(rc):
            return rc

        if rc in tag_db:
            sha=self.repo.cat_file("tag", rc).read().split(None, 2)[:2]
            if sha[0] != 'object':
                self.logger.debug("unexpected result from 'git-cat-file tag %s'" % rc)
                return None
            return sha[1]

        return None

    def shortrev(self, rev):
        "try to shorten sha id"
        #try to emulate the following:
        #return self.repo.rev_parse("--short", str(rev)).read().strip()

        rev = str(rev)

        db = self.get_commits()
        sdb = self._commit_db[3]

        if rev not in db:
            return rev

        srev = rev[:self.__SREV_MIN]
        srevs = sdb[srev]

        if len(srevs) == 1:
            return srev # we already got a unique id

        # find a shortened id for which rev doesn't conflict with
        # the other ones from srevs
        crevs = srevs - set([rev])

        for l in range(self.__SREV_MIN+1, 40):
            srev = rev[:l]
            if srev not in [ r[:l] for r in crevs ]:
                return srev

        return rev # worst-case, all except the last character match

    def get_branches(self):
        "returns list of (local) branches, with active (= HEAD) one being the first item"
        result=[]
        for e in self.repo.branch("-v", "--no-abbrev").readlines():
            (bname,bsha)=e[1:].strip().split()[:2]
            if e.startswith('*'):
                result.insert(0,(bname,bsha))
            else:
                result.append((bname,bsha))
        return result

    def get_tags(self):
        return [e.strip() for e in self.repo.tag("-l").readlines()]

    def ls_tree(self, rev, path=""):
        rev = str(rev) # paranoia
        if path.startswith('/'):
            path = path[1:]
        return [e.split(None, 3) for e in \
                    self.repo.ls_tree("-z", rev, "--", path).read().split('\0') if e]

    def read_commit(self, commit_id):
        if not commit_id:
            raise GitErrorCommit_Id

        commit_id = str(commit_id)

        db = self.get_commits()
        if commit_id not in db:
            self.logger.info("read_commit failed for '%s'" % commit_id)
            raise GitErrorSha

        with self.__commit_msg_lock:
            if self.__commit_msg_cache.has_key(commit_id):
                # cache hit
                result = self.__commit_msg_cache[commit_id]
                return result[0], dict(result[1])

            # cache miss
            raw = self.repo.cat_file("commit", commit_id).read()
            raw = unicode(raw, self.get_commit_encoding(), 'replace')
            lines = raw.splitlines()

            if not lines:
                raise GitErrorSha

            line = lines.pop(0)
            props = {}
            while line:
                (key,value) = line.split(None, 1)
                props.setdefault(key,[]).append(value.strip())
                line = lines.pop(0)

            result = ("\n".join(lines), props)

            self.__commit_msg_cache[commit_id] = result

            return result[0], dict(result[1])

    def get_file(self, sha):
        return self.repo.cat_file("blob", str(sha))

    def get_obj_size(self, sha):
        sha = str(sha)
        try:
            with self.__fs_obj_size_lock:
                if self.__fs_obj_size_cache.has_key(sha):
                    obj_size = self.__fs_obj_size_cache[sha]
                else:
                    obj_size = int(self.repo.cat_file("-s", sha).read().strip())
                    self.__fs_obj_size_cache[sha] = obj_size
        except ValueError:
            raise GitErrorSha("object '%s' not found" % sha)

        return obj_size

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

    def history(self, sha, path, limit=None):
        if limit is None:
            limit = -1
        for rev in self.repo.rev_list("--max-count=%d" % limit,
                                      str(sha), "--", path).readlines():
            yield rev.strip()

    def all_revs(self):
        return self.get_commits().iterkeys()

    def history_timerange(self, start, stop):
        for rev in self.repo.rev_list("--reverse",
                                      "--max-age=%d" % start,
                                      "--min-age=%d" % stop,
                                      "--all").readlines():
            yield rev.strip()

    def rev_is_anchestor_of(self, rev1, rev2):
        """return True if rev2 is successor of rev1"""
        rev1 = rev1.strip()
        rev2 = rev2.strip()
        return rev2 in self.children_recursive(rev1)

    def blame(self, commit_sha, path):
        in_metadata = False

        for line in self.repo.blame("-p", "--", path, str(commit_sha)).readlines():
            assert line
            if in_metadata:
                in_metadata = not line.startswith('\t')
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
        return self.repo.rev_list("--max-count=1", sha, "--", path).read().strip() or None

    def diff_tree(self, tree1, tree2, path="", find_renames=False):
        """calls `git diff-tree` and returns tuples of the kind
        (mode1,mode2,obj1,obj2,action,path1,path2)"""

        # diff-tree returns records with the following structure:
        # :<old-mode> <new-mode> <old-sha> <new-sha> <change> NUL <old-path> NUL [ <new-path> NUL ]

        diff_tree_args = ["-z", "-r"]
        if find_renames:
            diff_tree_args.append("-M")
        diff_tree_args.extend([str(tree1) if tree1 else "--root",
                               str(tree2),
                               "--", path])

        lines = self.repo.diff_tree(*diff_tree_args).read().split('\0')

        assert lines[-1] == ""
        del lines[-1]

        if tree1 is None:
            # if only one tree-sha is given on commandline,
            # the first line is just the redundant tree-sha itself...
            assert not lines[0].startswith(':')
            del lines[0]

        chg = None

        def __chg_tuple():
            if len(chg) == 6:
                chg.append(None)
            assert len(chg) == 7
            return tuple(chg)

        for line in lines:
            if line.startswith(':'):
                if chg:
                    yield __chg_tuple()

                chg = line[1:].split()
                assert len(chg) == 5
            else:
                chg.append(line)

        if chg:
            yield __chg_tuple()

if __name__ == '__main__':
    import sys, logging, timeit

    print "git version [%s]" % str(Storage.git_version())

    g = Storage(sys.argv[1], logging)

    print "[%s]" % g.head()
    print g.ls_tree(g.head())
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

    #p = g.head()
    #revs = [ g.history_relative_rev(p, i) for i in range(0,10) ]
    revs = g.get_commits().keys()

    def shortrev_test():
        for i in revs:
            i = str(i)
            s = g.shortrev(i)
            assert i.startswith(s)

    iters = 1
    print "timing %d*shortrev_test()..." % len(revs)
    t = timeit.Timer("shortrev_test()", "from __main__ import shortrev_test")
    print "%.2f usec/rev" % (1000000 * t.timeit(number=iters)/len(revs))

    #print len(check4loops(g.oldest_rev()))

    #print len(list(g.children_recursive(g.oldest_rev())))
