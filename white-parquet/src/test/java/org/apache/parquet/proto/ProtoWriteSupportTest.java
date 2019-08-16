/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.proto;

import com.google.protobuf.Message;
import com.tencent.dp.gdt.utils.WechatTrainingDataProtos;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

public class ProtoWriteSupportTest {

  private <T extends Message> ProtoWriteSupport<T> createReadConsumerInstance(Class<T> cls, RecordConsumer readConsumerMock) {
    ProtoWriteSupport support = new ProtoWriteSupport(cls);
    support.init(new Configuration());
    support.prepareForWrite(readConsumerMock);
    return support;
  }

  @Test
  public void testSimplestMessage() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(WechatTrainingDataProtos.WechatTrainingData.class, readConsumerMock);

    WechatTrainingDataProtos.WechatTrainingData.Builder msg = WechatTrainingDataProtos.WechatTrainingData.newBuilder();
    msg.setAdValuableCtr(10.1f);

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromString("oneValue"));
    inOrder.verify(readConsumerMock).endField("one", 0);

    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  public static void main(String[] args) throws IOException {
	  Path path = new Path("");
	  ProtoParquetWriter<WechatTrainingDataProtos.WechatTrainingData> ppwriter 
	  = new ProtoParquetWriter<WechatTrainingDataProtos.WechatTrainingData>(path,WechatTrainingDataProtos.WechatTrainingData.class);
  }
}
