// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package metanode

import (
	"fmt"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

// TODO: can't set this attr for out update xattr req
const innerDirLockKey = "cfs_inner_xattr_dir_lock_key"

type ExtendOpResult struct {
	Status uint8
	Extend *Extend
}

func (mp *metaPartition) fsmLockDir(req *proto.LockDirRequest) (resp *proto.LockDirResponse) {
	if req.Lease == 0 {
		return mp.fsmUnlockDir(req)
	}

	mp.xattrLock.Lock()
	defer mp.xattrLock.Unlock()

	resp = &proto.LockDirResponse{
		Status: proto.OpOk,
		LockId: req.LockId,
	}

	newExpire := req.SubmitTime.Unix() + int64(req.Lease)
	newVal := fmt.Sprintf("%d|%d", req.LockId, newExpire)

	log.LogDebugf("fsmLockDir: req info %s, val %s", req.String(), newVal)

	var newExtend = NewExtend(req.Inode)
	treeItem := mp.extendTree.CopyGet(newExtend)

	var oldValue []byte
	var existExtend *Extend

	if treeItem == nil {
		newExtend.Put([]byte(innerDirLockKey), []byte(newVal))
		mp.extendTree.ReplaceOrInsert(newExtend, true)
		return
	}

	existExtend = treeItem.(*Extend)
	oldValue, _ = existExtend.Get([]byte(innerDirLockKey))
	if oldValue == nil {
		newExtend.Put([]byte(innerDirLockKey), []byte(newVal))
		existExtend.Merge(newExtend, true)
		return
	}

	var oldLkId, oldExpire int64
	_, err := fmt.Sscanf(string(oldValue), "%d|%d", &oldLkId, &oldExpire)
	if err != nil {
		log.LogErrorf("fsmLockDir: parse req failed, req %s, old %s, err %s", req.String(), string(oldValue), err.Error())
		resp.Status = proto.OpExistErr
		return 
	}

	log.LogDebugf("fsmLockDir: get old lock dir info, req %v, old %d, expire %d", req, oldLkId, oldExpire)

	if req.LockId == oldLkId {
		newExtend.Put([]byte(innerDirLockKey), []byte(newVal))
		existExtend.Merge(newExtend, true)
		return
	}

	if req.SubmitTime.Unix() < oldExpire {
		resp.Status = proto.OpExistErr
		return
	}

	newExtend.Put([]byte(innerDirLockKey), []byte(newVal))
	existExtend.Merge(newExtend, true)
	return
}

func (mp *metaPartition) fsmUnlockDir(req *proto.LockDirRequest) (resp *proto.LockDirResponse) {
	mp.xattrLock.Lock()
	defer mp.xattrLock.Unlock()

	resp = &proto.LockDirResponse{
		Status: proto.OpOk,
		LockId: req.LockId,
	}

	newExpire := req.SubmitTime.Unix() + int64(req.Lease)
	newVal := fmt.Sprintf("%d|%d", req.LockId, newExpire)
	log.LogDebugf("fsmUnlockDir: req info %s, val %s", req, newVal)

	var newExtend = NewExtend(req.Inode)
	treeItem := mp.extendTree.CopyGet(newExtend)

	var oldValue []byte
	var existExtend *Extend

	if treeItem == nil {
		log.LogWarnf("fsmUnlockDir: lock not exist, no need to unlock, req %s", req.String())
		return
	}

	existExtend = treeItem.(*Extend)
	oldValue, _ = existExtend.Get([]byte(innerDirLockKey))
	if oldValue == nil {
		log.LogWarnf("fsmUnlockDir: target lock val not exist, no need to unlock, req %s", req.String())
		return
	}

	var oldLkId, oldExpire int64
	_, err := fmt.Sscanf(string(oldValue), "%d|%d", &oldLkId, &oldExpire)
	if err != nil {
		log.LogErrorf("fsmUnlockDir: parse req failed, req %s, old %s, err %s", req.String(), string(oldValue), err.Error())
		resp.Status = proto.OpExistErr
		return
	}

	log.LogDebugf("fsmUnlockDir: get old lock dir info, req %v, old %d, expire %d", req, oldLkId, oldExpire)

	if req.LockId != oldLkId {
		log.LogWarnf("fsmUnlockDir: already been locked by other, req %v, old %d", req.String(), oldLkId)
		resp.Status = proto.OpExistErr
		return
	}

	existExtend.Remove([]byte(innerDirLockKey))
	return
}

func (mp *metaPartition) fsmSetXAttr(extend *Extend) (err error) {

	treeItem := mp.extendTree.CopyGet(extend)
	var e *Extend
	if treeItem == nil {
		mp.extendTree.ReplaceOrInsert(extend, true)
	} else {
		e = treeItem.(*Extend)
		e.Merge(extend, true)
	}

	return
}

func (mp *metaPartition) fsmRemoveXAttr(extend *Extend) (err error) {
	treeItem := mp.extendTree.CopyGet(extend)
	if treeItem == nil {
		return
	}
	e := treeItem.(*Extend)
	extend.Range(func(key, value []byte) bool {
		e.Remove(key)
		return true
	})
	return
}
