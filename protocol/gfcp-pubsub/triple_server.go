/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gfcp_pubsub

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"reflect"
	"sync"
)

import (
	hessian "github.com/apache/dubbo-go-hessian2"

	"github.com/dubbogo/grpc-go"
	"github.com/dubbogo/grpc-go/encoding"
	"github.com/dubbogo/grpc-go/encoding/proto_wrapper_api"
	"github.com/dubbogo/grpc-go/encoding/raw_proto"

	perrors "github.com/pkg/errors"
)

import (
	"github.com/dubbogo/triple/pkg/common"
	"github.com/dubbogo/triple/pkg/config"
)

// TripleServer is the object that can be started and listening remote request
type TripleServer struct {
	grpcServer    *RedisPubsubServer
	rpcServiceMap *sync.Map
	registeredKey map[string]bool
	// config
	opt *config.Option
}

// NewTripleServer can create Server with url and some user impl providers stored in @serviceMap
// @serviceMap should be sync.Map: "interfaceKey" -> Dubbo3GrpcService
func NewTripleServer(serviceMap *sync.Map, opt *config.Option) *TripleServer {
	if opt == nil {
		opt = config.NewTripleOption()
	}
	return &TripleServer{
		rpcServiceMap: serviceMap,
		opt:           opt,
		registeredKey: make(map[string]bool),
	}
}

// Stop
func (t *TripleServer) Stop() {
	t.grpcServer.Stop()
}

/*
var Greeter_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "api.Greeter",
	HandlerType: (*GreeterServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "SayHello",
			Handler:    _Greeter_SayHello_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "SayHelloStream",
			Handler:       _Greeter_SayHelloStream_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "samples_api.proto",
}

*/

/*

func _Greeter_SayHello_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HelloRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	base := srv.(dubbo3.Dubbo3GrpcService)
	args := []interface{}{}
	args = append(args, in)
	invo := invocation.NewRPCInvocation("SayHello", args, nil)
	if interceptor == nil {
		result := base.XXX_GetProxyImpl().Invoke(ctx, invo)
		return result, result.Error()
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.Greeter/SayHello",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GreeterServer).SayHello(ctx, req.(*HelloRequest))
	}
	return interceptor(ctx, in, info, handler)
}
*/

func newGenericCodec() common.GenericCodec {
	return &GenericCodec{
		codec: raw_proto.NewProtobufCodec(),
	}
}

// GenericCodec is pb impl of TwoWayCodec
type GenericCodec struct {
	codec encoding.Codec
}

// UnmarshalRequest unmarshal bytes @data to interface
func (h *GenericCodec) UnmarshalRequest(data []byte) ([]interface{}, error) {
	wrapperRequest := proto_wrapper_api.TripleRequestWrapper{}
	err := h.codec.Unmarshal(data, &wrapperRequest)
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, 0, len(wrapperRequest.Args))

	for _, value := range wrapperRequest.Args {
		decoder := hessian.NewDecoder(value)
		val, err := decoder.Decode()
		if err != nil {
			return nil, err
		}
		result = append(result, val)
	}
	return result, nil
}

func createGrpcDesc(serviceName string, service common.TripleUnaryService) *grpc.ServiceDesc {
	genericCodec := newGenericCodec()
	return &grpc.ServiceDesc{
		ServiceName: serviceName,
		HandlerType: (*common.TripleUnaryService)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "InvokeWithArgs",
				Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
					methodName := ctx.Value("XXX_TRIPLE_GO_METHOD_NAME").(string)
					genericPayload, ok := ctx.Value("XXX_TRIPLE_GO_GENERIC_PAYLOAD").([]byte)
					base := srv.(common.TripleUnaryService)
					if methodName == "$invoke" && ok {
						args, err := genericCodec.UnmarshalRequest(genericPayload)
						if err != nil {
							return nil, perrors.Errorf("unaryProcessor.processUnaryRPC: generic invoke with request %s unmarshal error = %s", string(genericPayload), err.Error())
						}
						return base.InvokeWithArgs(ctx, methodName, args)
					} else {

						reqParam, ok := service.GetReqParamsInterfaces(methodName)
						if !ok {
							return nil, perrors.Errorf("method name %s is not provided by service, please check if correct", methodName)
						}
						if e := dec(reqParam); e != nil {
							return nil, e
						}
						args := make([]interface{}, 0, len(reqParam))
						for _, v := range reqParam {
							tempParamObj := reflect.ValueOf(v).Elem().Interface()
							args = append(args, tempParamObj)
						}
						return base.InvokeWithArgs(ctx, methodName, args)
					}
				},
			},
		},
	}
}

type ServiceUnit struct {
	desc    *grpc.ServiceDesc
	service interface{}
}

// serviceInfo wraps information about a service. It is very similar to
// ServiceDesc and is constructed from it for internal purposes.
type serviceInfo struct {
	// Contains the implementation for the methods in this service.
	serviceImpl interface{}
	methods     map[string]*grpc.MethodDesc
	streams     map[string]*grpc.StreamDesc
	mdata       interface{}
}

type RedisPubsubServer struct {
	serviceInfoMap map[string]*serviceInfo // service name -> service info
	cancelMap      sync.Map
}

func (r *RedisPubsubServer) RegisterService(sd *grpc.ServiceDesc, ss interface{}) {
	info := &serviceInfo{
		serviceImpl: ss,
		methods:     make(map[string]*grpc.MethodDesc),
		streams:     make(map[string]*grpc.StreamDesc),
		mdata:       sd.Metadata,
	}
	for i := range sd.Methods {
		d := &sd.Methods[i]
		info.methods[d.MethodName] = d
	}
	for i := range sd.Streams {
		d := &sd.Streams[i]
		info.streams[d.StreamName] = d
	}
	r.serviceInfoMap[sd.ServiceName] = info
}

func (r *RedisPubsubServer) Serve(address string) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	// subscribe all method
	wg := sync.WaitGroup{}
	for interfaceName, serviceInfo := range r.serviceInfoMap {
		for _, method := range serviceInfo.methods {
			wg.Add(1)
			ctx, cancel := context.WithCancel(context.Background())
			subscribeKey := fmt.Sprintf("/%s/%s", interfaceName, method.MethodName)
			r.cancelMap.Store(subscribeKey, cancel)
			subChan := rdb.Subscribe(ctx, subscribeKey).Channel()
			go func(ctx context.Context, subChan <-chan *redis.Message, methodHandler grpc.MethodDesc, interfaceName string, serviceImpl interface{}) {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case msg := <-subChan:
						d := []byte(msg.Payload)
						df := func(v interface{}) error {
							return raw_proto.NewProtobufCodec().Unmarshal(d, v)
						}
						ctx := context.Background()
						ctx = context.WithValue(ctx, "XXX_TRIPLE_GO_METHOD_NAME", methodHandler.MethodName)
						ctx = context.WithValue(ctx, "XXX_TRIPLE_GO_INTERFACE_NAME", interfaceName)
						ctx = context.WithValue(ctx, "XXX_TRIPLE_GO_GENERIC_PAYLOAD", d)
						_, err := methodHandler.Handler(serviceImpl, ctx, df, nil)
						if err != nil {
							log.Println("gfcp-pubsub handle event error = ", err)
						}
					}

				}
			}(ctx, subChan, *method, interfaceName, serviceInfo.serviceImpl)
		}
	}
	wg.Wait()
}

func (r *RedisPubsubServer) Stop() {
	r.cancelMap.Range(func(key, value interface{}) bool {
		value.(context.CancelFunc)()
		return true
	})
	r.serviceInfoMap = map[string]*serviceInfo{}
	r.cancelMap = sync.Map{}
}

func newGrpcServerWithCodec(opt *config.Option) *RedisPubsubServer {
	return &RedisPubsubServer{
		serviceInfoMap: map[string]*serviceInfo{},
	}
}

// Start can start a triple server
func (t *TripleServer) Start() {
	grpcServer := newGrpcServerWithCodec(t.opt)
	t.rpcServiceMap.Range(func(key, value interface{}) bool {
		t.registeredKey[key.(string)] = true
		grpcService, ok := value.(common.TripleGrpcService)
		if ok {
			desc := grpcService.XXX_ServiceDesc()
			desc.ServiceName = key.(string)
			grpcServer.RegisterService(desc, value)
		} else {
			desc := createGrpcDesc(key.(string), value.(common.TripleUnaryService))
			grpcServer.RegisterService(desc, value)
		}
		//if key == "grpc.reflection.v1alpha.ServerReflection" {
		//grpcService.(common.TripleGrpcReflectService).SetGRPCServer(grpcServer)
		//}
		return true
	})

	go grpcServer.Serve(t.opt.Location)
	t.grpcServer = grpcServer
}

func (t *TripleServer) RefreshService() {
	t.opt.Logger.Debugf("TripleServer.Refresh: call refresh services")
	grpcServer := newGrpcServerWithCodec(t.opt)
	t.rpcServiceMap.Range(func(key, value interface{}) bool {
		grpcService, _ := value.(common.TripleGrpcService)
		desc := grpcService.XXX_ServiceDesc()
		desc.ServiceName = key.(string)
		grpcServer.RegisterService(desc, value)
		return true
	})
	t.grpcServer.Stop()
	go grpcServer.Serve(t.opt.Location)
	t.grpcServer = grpcServer
}
