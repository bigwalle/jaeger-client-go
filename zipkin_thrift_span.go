// Copyright (c) 2017 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jaeger

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/opentracing/opentracing-go/ext"

	"github.com/welcome112s/jaeger-client-go/internal/spanlog"
	z "github.com/welcome112s/jaeger-client-go/thrift-gen/zipkincore"
	"github.com/welcome112s/jaeger-client-go/utils"
)

const (
	// Zipkin UI does not work well with non-string tag values
	allowPackedNumbers = false
)

var specialTagHandlers = map[string]func(*zipkinSpan, interface{}){
	string(ext.SpanKind):     setSpanKind,
	string(ext.PeerHostIPv4): setPeerIPv4,
	string(ext.PeerPort):     setPeerPort,
	string(ext.PeerService):  setPeerService,
	TracerIPTagKey:           removeTag,
}

// BuildZipkinThrift builds thrift span based on internal span.
func BuildZipkinThrift(s *Span) *z.Span {
	span := &zipkinSpan{Span: s}
	span.handleSpecialTags()
	parentID := int64(span.ContextV1.ParentID)
	var ptrParentID *int64
	if parentID != 0 {
		ptrParentID = &parentID
	}
	traceIDHigh := int64(span.ContextV1.TraceID.High)
	var ptrTraceIDHigh *int64
	if traceIDHigh != 0 {
		ptrTraceIDHigh = &traceIDHigh
	}
	timestamp := utils.TimeToMicrosecondsSinceEpochInt64(span.StartTime)
	duration := span.Duration.Nanoseconds() / int64(time.Microsecond)
	endpoint := &z.Endpoint{
		ServiceName: span.TracerV1.serviceName,
		Ipv4:        int32(span.TracerV1.hostIPv4)}
	thriftSpan := &z.Span{
		TraceID:           int64(span.ContextV1.TraceID.Low),
		TraceIDHigh:       ptrTraceIDHigh,
		ID:                int64(span.ContextV1.SpanID),
		ParentID:          ptrParentID,
		Name:              span.OperationName,
		Timestamp:         &timestamp,
		Duration:          &duration,
		Debug:             span.ContextV1.IsDebug(),
		Annotations:       buildAnnotations(span, endpoint),
		BinaryAnnotations: buildBinaryAnnotations(span, endpoint)}
	return thriftSpan
}

func buildAnnotations(span *zipkinSpan, endpoint *z.Endpoint) []*z.Annotation {
	// automatically adding 2 Zipkin CoreAnnotations
	annotations := make([]*z.Annotation, 0, 2+len(span.Logs))
	var startLabel, endLabel string
	if span.spanKind == string(ext.SpanKindRPCClientEnum) {
		startLabel, endLabel = z.CLIENT_SEND, z.CLIENT_RECV
	} else if span.spanKind == string(ext.SpanKindRPCServerEnum) {
		startLabel, endLabel = z.SERVER_RECV, z.SERVER_SEND
	}
	if !span.StartTime.IsZero() && startLabel != "" {
		start := &z.Annotation{
			Timestamp: utils.TimeToMicrosecondsSinceEpochInt64(span.StartTime),
			Value:     startLabel,
			Host:      endpoint}
		annotations = append(annotations, start)
		if span.Duration != 0 {
			endTs := span.StartTime.Add(span.Duration)
			end := &z.Annotation{
				Timestamp: utils.TimeToMicrosecondsSinceEpochInt64(endTs),
				Value:     endLabel,
				Host:      endpoint}
			annotations = append(annotations, end)
		}
	}
	for _, log := range span.Logs {
		anno := &z.Annotation{
			Timestamp: utils.TimeToMicrosecondsSinceEpochInt64(log.Timestamp),
			Host:      endpoint}
		if content, err := spanlog.MaterializeWithJSON(log.Fields); err == nil {
			anno.Value = truncateString(string(content), span.TracerV1.options.maxTagValueLength)
		} else {
			anno.Value = err.Error()
		}
		annotations = append(annotations, anno)
	}
	return annotations
}

func buildBinaryAnnotations(span *zipkinSpan, endpoint *z.Endpoint) []*z.BinaryAnnotation {
	// automatically adding local component or server/client address tag, and client version
	annotations := make([]*z.BinaryAnnotation, 0, 2+len(span.Tags))

	if span.peerDefined() && span.isRPC() {
		peer := z.Endpoint{
			Ipv4:        span.peer.Ipv4,
			Port:        span.peer.Port,
			ServiceName: span.peer.ServiceName}
		label := z.CLIENT_ADDR
		if span.isRPCClient() {
			label = z.SERVER_ADDR
		}
		anno := &z.BinaryAnnotation{
			Key:            label,
			Value:          []byte{1},
			AnnotationType: z.AnnotationType_BOOL,
			Host:           &peer}
		annotations = append(annotations, anno)
	}
	if !span.isRPC() {
		componentName := endpoint.ServiceName
		for _, tag := range span.Tags {
			if tag.key == string(ext.Component) {
				componentName = stringify(tag.value)
				break
			}
		}
		local := &z.BinaryAnnotation{
			Key:            z.LOCAL_COMPONENT,
			Value:          []byte(componentName),
			AnnotationType: z.AnnotationType_STRING,
			Host:           endpoint}
		annotations = append(annotations, local)
	}
	for _, tag := range span.Tags {
		// "Special tags" are already handled by this point, we'd be double reporting the
		// tags if we don't skip here
		if _, ok := specialTagHandlers[tag.key]; ok {
			continue
		}
		if anno := buildBinaryAnnotation(tag.key, tag.value, span.TracerV1.options.maxTagValueLength, nil); anno != nil {
			annotations = append(annotations, anno)
		}
	}
	return annotations
}

func buildBinaryAnnotation(key string, val interface{}, maxTagValueLength int, endpoint *z.Endpoint) *z.BinaryAnnotation {
	bann := &z.BinaryAnnotation{Key: key, Host: endpoint}
	if value, ok := val.(string); ok {
		bann.Value = []byte(truncateString(value, maxTagValueLength))
		bann.AnnotationType = z.AnnotationType_STRING
	} else if value, ok := val.([]byte); ok {
		if len(value) > maxTagValueLength {
			value = value[:maxTagValueLength]
		}
		bann.Value = value
		bann.AnnotationType = z.AnnotationType_BYTES
	} else if value, ok := val.(int32); ok && allowPackedNumbers {
		bann.Value = int32ToBytes(value)
		bann.AnnotationType = z.AnnotationType_I32
	} else if value, ok := val.(int64); ok && allowPackedNumbers {
		bann.Value = int64ToBytes(value)
		bann.AnnotationType = z.AnnotationType_I64
	} else if value, ok := val.(int); ok && allowPackedNumbers {
		bann.Value = int64ToBytes(int64(value))
		bann.AnnotationType = z.AnnotationType_I64
	} else if value, ok := val.(bool); ok {
		bann.Value = []byte{boolToByte(value)}
		bann.AnnotationType = z.AnnotationType_BOOL
	} else {
		value := stringify(val)
		bann.Value = []byte(truncateString(value, maxTagValueLength))
		bann.AnnotationType = z.AnnotationType_STRING
	}
	return bann
}

func stringify(value interface{}) string {
	if s, ok := value.(string); ok {
		return s
	}
	return fmt.Sprintf("%+v", value)
}

func truncateString(value string, maxLength int) string {
	// we ignore the problem of utf8 runes possibly being sliced in the middle,
	// as it is rather expensive to iterate through each tag just to find rune
	// boundaries.
	if len(value) > maxLength {
		return value[:maxLength]
	}
	return value
}

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// int32ToBytes converts int32 to bytes.
func int32ToBytes(i int32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(i))
	return buf
}

// int64ToBytes converts int64 to bytes.
func int64ToBytes(i int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

type zipkinSpan struct {
	*Span

	// peer points to the peer service participating in this span,
	// e.g. the Client if this span is a server span,
	// or Server if this span is a client span
	peer struct {
		Ipv4        int32
		Port        int16
		ServiceName string
	}

	// used to distinguish local vs. RPC Server vs. RPC Client spans
	spanKind string
}

func (s *zipkinSpan) handleSpecialTags() {
	s.Lock()
	defer s.Unlock()
	if s.FirstInProcess {
		// append the process tags
		s.Tags = append(s.Tags, s.TracerV1.tags...)
	}
	filteredTags := make([]Tag, 0, len(s.Tags))
	for _, tag := range s.Tags {
		if handler, ok := specialTagHandlers[tag.key]; ok {
			handler(s, tag.value)
		} else {
			filteredTags = append(filteredTags, tag)
		}
	}
	s.Tags = filteredTags
}

func setSpanKind(s *zipkinSpan, value interface{}) {
	if val, ok := value.(string); ok {
		s.spanKind = val
		return
	}
	if val, ok := value.(ext.SpanKindEnum); ok {
		s.spanKind = string(val)
	}
}

func setPeerIPv4(s *zipkinSpan, value interface{}) {
	if val, ok := value.(string); ok {
		if ip, err := utils.ParseIPToUint32(val); err == nil {
			s.peer.Ipv4 = int32(ip)
			return
		}
	}
	if val, ok := value.(uint32); ok {
		s.peer.Ipv4 = int32(val)
		return
	}
	if val, ok := value.(int32); ok {
		s.peer.Ipv4 = val
	}
}

func setPeerPort(s *zipkinSpan, value interface{}) {
	if val, ok := value.(string); ok {
		if port, err := utils.ParsePort(val); err == nil {
			s.peer.Port = int16(port)
			return
		}
	}
	if val, ok := value.(uint16); ok {
		s.peer.Port = int16(val)
		return
	}
	if val, ok := value.(int); ok {
		s.peer.Port = int16(val)
	}
}

func setPeerService(s *zipkinSpan, value interface{}) {
	if val, ok := value.(string); ok {
		s.peer.ServiceName = val
	}
}

func removeTag(s *zipkinSpan, value interface{}) {}

func (s *zipkinSpan) peerDefined() bool {
	return s.peer.ServiceName != "" || s.peer.Ipv4 != 0 || s.peer.Port != 0
}

func (s *zipkinSpan) isRPC() bool {
	s.RLock()
	defer s.RUnlock()
	return s.spanKind == string(ext.SpanKindRPCClientEnum) || s.spanKind == string(ext.SpanKindRPCServerEnum)
}

func (s *zipkinSpan) isRPCClient() bool {
	s.RLock()
	defer s.RUnlock()
	return s.spanKind == string(ext.SpanKindRPCClientEnum)
}
