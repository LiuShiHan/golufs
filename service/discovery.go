package main

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/resolver"
	"time"
)

type EtcdServiceDiscovery struct {
	client      *clientv3.Client
	serviceName string
	instanceID  string
	endpoints   []string
	leaseID     clientv3.LeaseID
	stopChan    chan struct{}
}

func NewEtcdServiceDiscovery(endpoints []string, serviceName, instanceID string) (*EtcdServiceDiscovery, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("create etcd service discovery client error: %v", err)
	}

	return &EtcdServiceDiscovery{
		client:      client,
		serviceName: serviceName,
		instanceID:  instanceID,
		endpoints:   endpoints,
		stopChan:    make(chan struct{}),
	}, nil

}

func (d *EtcdServiceDiscovery) Register(ctx context.Context, address string, ttl int) error {
	resp, err := d.client.Grant(ctx, int64(ttl))
	if err != nil {
		return fmt.Errorf("register etcd service discovery grant error: %v", err)
	}
	d.leaseID = resp.ID
	key := fmt.Sprintf("/service/%s/%s", d.serviceName, d.instanceID)
	value := address

	_, err = d.client.Put(ctx, key, value, clientv3.WithLease(d.leaseID))
	if err != nil {
		return fmt.Errorf("register etcd service discovery Put error: %v", err)
	}
	go d.keepAlive(ctx)
	return nil
}

func (d *EtcdServiceDiscovery) Unregister(ctx context.Context) error {
	close(d.stopChan)
	if d.leaseID != 0 {
		_, err := d.client.Revoke(ctx, d.leaseID)
		if err != nil {
			return fmt.Errorf("unregister etcd service discovery revoke error: %v", err)
		}
	}
	return d.client.Close()
}

func (d *EtcdServiceDiscovery) keepAlive(ctx context.Context) { //TODO: 这个还要看一下
	ch, err := d.client.KeepAlive(ctx, d.leaseID)
	if err != nil {
		fmt.Printf("keep alive etcd service discovery keepAlive error: %v", err)
		return
	}

	for {
		select {
		case <-d.stopChan:
			return
		case <-ctx.Done():
			return
		case ka := <-ch:
			if ka == nil {
				fmt.Println("keep alive channel closed")
				return
			}

		}
	}

}

type EtcdResolver struct {
	client      *clientv3.Client
	serviceName string
}

func NewEtcdResolver(endpoints []string, serviceName string) (*EtcdResolver, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return &EtcdResolver{
		client:      client,
		serviceName: serviceName,
	}, nil

}

//func (r *EtcdResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
//
//}

func (r *EtcdResolver) Scheme() string {
	return "etcd"
}

type etcdResolver struct {
	client      *clientv3.Client
	serviceName string
	cc          resolver.ClientConn
}

func (r *etcdResolver) watch() {
	ctx := context.Background()
	keyPrefix := fmt.Sprintf("/service/%s", r.serviceName)

	resp, err := r.client.Get(ctx, keyPrefix, clientv3.WithPrefix())
	print(resp, err)
}

func main() {
	c := clientv3.Client{
		Cluster:     nil,
		KV:          nil,
		Lease:       nil,
		Watcher:     nil,
		Auth:        nil,
		Maintenance: nil,
		Username:    "",
		Password:    "",
	}
	fmt.Println(c)
}
