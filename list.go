package client

import (
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"strings"
)

// list a directory
func (clt *EtcdHRCHYClient) List(key string) ([]*Node, error) {
	key, _, err := clt.ensureKey(key)
	if err != nil {
		return nil, err
	}
	// directory start with /
	dir := key + "/"

	txn := clt.client.Txn(clt.ctx)
	// make sure the list key is a directory

	txn.If(
		clientv3.Compare(
			clientv3.Value(key),
			"=",
			clt.dirValue,
		),
	).Then(
		clientv3.OpGet(dir, clientv3.WithPrefix()),
	).Else(
		clientv3.OpGet(dir, clientv3.WithPrefix()),
	)

	txnResp, err := txn.Commit()
	if err != nil {
		return nil, err
	}

	if !txnResp.Succeeded {
		if len(txnResp.Responses) > 0 {
			rangeResp := txnResp.Responses[0].GetResponseRange()
			return clt.list(dir, rangeResp.Kvs)
		} else {
			// empty directory
			return []*Node{}, nil
		}
		//return nil, ErrorListKey
	} else {
		if len(txnResp.Responses) > 0 {
			rangeResp := txnResp.Responses[0].GetResponseRange()
			return clt.list(dir, rangeResp.Kvs)
		} else {
			// empty directory
			return []*Node{}, nil
		}
	}
}

// pick key/value under the dir
func (clt *EtcdHRCHYClient) list(dir string, kvs []*mvccpb.KeyValue) ([]*Node, error) {
	nodes := []*Node{}
	for _, kv := range kvs {
		name := strings.TrimPrefix(string(kv.Key), dir)
		//多级目录 因为V3 版本是扁平化设计、用户预先已经存在/dirA/dirB/dirC/ 的情况、但是在web ui中不显示
		//以下加载展示 用户提前写入到etcd的配置
		if strings.Contains(name, "/") {
			// secondary directory
			//first '/' found position
			pos := strings.Index(name, "/")
			if pos != -1 {
				name = string(kv.Key)[:len(dir)+pos]
				kv.Key = []byte(name)
				kv.Value = []byte(clt.dirValue)
				node := clt.createNode(kv)
				node.IsDir = true
				nodes = append(nodes, node)
			}
			continue
		} else {
			nodes = append(nodes, clt.createNode(kv))
		}

	}
	return nodes, nil
}
