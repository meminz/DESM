package plant.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.72.0)",
    comments = "Source: plant_comms.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class PlantCommunicationGrpc {

  private PlantCommunicationGrpc() {}

  public static final java.lang.String SERVICE_NAME = "plantcomms.PlantCommunication";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<plant.grpc.PlantComms.GreetingsMessage,
      plant.grpc.PlantComms.GreetingsResponse> getSendGreetingsMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SendGreetingsMessage",
      requestType = plant.grpc.PlantComms.GreetingsMessage.class,
      responseType = plant.grpc.PlantComms.GreetingsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<plant.grpc.PlantComms.GreetingsMessage,
      plant.grpc.PlantComms.GreetingsResponse> getSendGreetingsMessageMethod() {
    io.grpc.MethodDescriptor<plant.grpc.PlantComms.GreetingsMessage, plant.grpc.PlantComms.GreetingsResponse> getSendGreetingsMessageMethod;
    if ((getSendGreetingsMessageMethod = PlantCommunicationGrpc.getSendGreetingsMessageMethod) == null) {
      synchronized (PlantCommunicationGrpc.class) {
        if ((getSendGreetingsMessageMethod = PlantCommunicationGrpc.getSendGreetingsMessageMethod) == null) {
          PlantCommunicationGrpc.getSendGreetingsMessageMethod = getSendGreetingsMessageMethod =
              io.grpc.MethodDescriptor.<plant.grpc.PlantComms.GreetingsMessage, plant.grpc.PlantComms.GreetingsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SendGreetingsMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  plant.grpc.PlantComms.GreetingsMessage.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  plant.grpc.PlantComms.GreetingsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new PlantCommunicationMethodDescriptorSupplier("SendGreetingsMessage"))
              .build();
        }
      }
    }
    return getSendGreetingsMessageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<plant.grpc.PlantComms.ElectionMessage,
      plant.grpc.PlantComms.ElectionResponse> getSendElectionMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SendElectionMessage",
      requestType = plant.grpc.PlantComms.ElectionMessage.class,
      responseType = plant.grpc.PlantComms.ElectionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<plant.grpc.PlantComms.ElectionMessage,
      plant.grpc.PlantComms.ElectionResponse> getSendElectionMessageMethod() {
    io.grpc.MethodDescriptor<plant.grpc.PlantComms.ElectionMessage, plant.grpc.PlantComms.ElectionResponse> getSendElectionMessageMethod;
    if ((getSendElectionMessageMethod = PlantCommunicationGrpc.getSendElectionMessageMethod) == null) {
      synchronized (PlantCommunicationGrpc.class) {
        if ((getSendElectionMessageMethod = PlantCommunicationGrpc.getSendElectionMessageMethod) == null) {
          PlantCommunicationGrpc.getSendElectionMessageMethod = getSendElectionMessageMethod =
              io.grpc.MethodDescriptor.<plant.grpc.PlantComms.ElectionMessage, plant.grpc.PlantComms.ElectionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SendElectionMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  plant.grpc.PlantComms.ElectionMessage.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  plant.grpc.PlantComms.ElectionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new PlantCommunicationMethodDescriptorSupplier("SendElectionMessage"))
              .build();
        }
      }
    }
    return getSendElectionMessageMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static PlantCommunicationStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<PlantCommunicationStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<PlantCommunicationStub>() {
        @java.lang.Override
        public PlantCommunicationStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new PlantCommunicationStub(channel, callOptions);
        }
      };
    return PlantCommunicationStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static PlantCommunicationBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<PlantCommunicationBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<PlantCommunicationBlockingV2Stub>() {
        @java.lang.Override
        public PlantCommunicationBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new PlantCommunicationBlockingV2Stub(channel, callOptions);
        }
      };
    return PlantCommunicationBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static PlantCommunicationBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<PlantCommunicationBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<PlantCommunicationBlockingStub>() {
        @java.lang.Override
        public PlantCommunicationBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new PlantCommunicationBlockingStub(channel, callOptions);
        }
      };
    return PlantCommunicationBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static PlantCommunicationFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<PlantCommunicationFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<PlantCommunicationFutureStub>() {
        @java.lang.Override
        public PlantCommunicationFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new PlantCommunicationFutureStub(channel, callOptions);
        }
      };
    return PlantCommunicationFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void sendGreetingsMessage(plant.grpc.PlantComms.GreetingsMessage request,
        io.grpc.stub.StreamObserver<plant.grpc.PlantComms.GreetingsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSendGreetingsMessageMethod(), responseObserver);
    }

    /**
     */
    default void sendElectionMessage(plant.grpc.PlantComms.ElectionMessage request,
        io.grpc.stub.StreamObserver<plant.grpc.PlantComms.ElectionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSendElectionMessageMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service PlantCommunication.
   */
  public static abstract class PlantCommunicationImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return PlantCommunicationGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service PlantCommunication.
   */
  public static final class PlantCommunicationStub
      extends io.grpc.stub.AbstractAsyncStub<PlantCommunicationStub> {
    private PlantCommunicationStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PlantCommunicationStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new PlantCommunicationStub(channel, callOptions);
    }

    /**
     */
    public void sendGreetingsMessage(plant.grpc.PlantComms.GreetingsMessage request,
        io.grpc.stub.StreamObserver<plant.grpc.PlantComms.GreetingsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSendGreetingsMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void sendElectionMessage(plant.grpc.PlantComms.ElectionMessage request,
        io.grpc.stub.StreamObserver<plant.grpc.PlantComms.ElectionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSendElectionMessageMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service PlantCommunication.
   */
  public static final class PlantCommunicationBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<PlantCommunicationBlockingV2Stub> {
    private PlantCommunicationBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PlantCommunicationBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new PlantCommunicationBlockingV2Stub(channel, callOptions);
    }

    /**
     */
    public plant.grpc.PlantComms.GreetingsResponse sendGreetingsMessage(plant.grpc.PlantComms.GreetingsMessage request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendGreetingsMessageMethod(), getCallOptions(), request);
    }

    /**
     */
    public plant.grpc.PlantComms.ElectionResponse sendElectionMessage(plant.grpc.PlantComms.ElectionMessage request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendElectionMessageMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service PlantCommunication.
   */
  public static final class PlantCommunicationBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<PlantCommunicationBlockingStub> {
    private PlantCommunicationBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PlantCommunicationBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new PlantCommunicationBlockingStub(channel, callOptions);
    }

    /**
     */
    public plant.grpc.PlantComms.GreetingsResponse sendGreetingsMessage(plant.grpc.PlantComms.GreetingsMessage request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendGreetingsMessageMethod(), getCallOptions(), request);
    }

    /**
     */
    public plant.grpc.PlantComms.ElectionResponse sendElectionMessage(plant.grpc.PlantComms.ElectionMessage request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendElectionMessageMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service PlantCommunication.
   */
  public static final class PlantCommunicationFutureStub
      extends io.grpc.stub.AbstractFutureStub<PlantCommunicationFutureStub> {
    private PlantCommunicationFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PlantCommunicationFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new PlantCommunicationFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<plant.grpc.PlantComms.GreetingsResponse> sendGreetingsMessage(
        plant.grpc.PlantComms.GreetingsMessage request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSendGreetingsMessageMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<plant.grpc.PlantComms.ElectionResponse> sendElectionMessage(
        plant.grpc.PlantComms.ElectionMessage request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSendElectionMessageMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SEND_GREETINGS_MESSAGE = 0;
  private static final int METHODID_SEND_ELECTION_MESSAGE = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SEND_GREETINGS_MESSAGE:
          serviceImpl.sendGreetingsMessage((plant.grpc.PlantComms.GreetingsMessage) request,
              (io.grpc.stub.StreamObserver<plant.grpc.PlantComms.GreetingsResponse>) responseObserver);
          break;
        case METHODID_SEND_ELECTION_MESSAGE:
          serviceImpl.sendElectionMessage((plant.grpc.PlantComms.ElectionMessage) request,
              (io.grpc.stub.StreamObserver<plant.grpc.PlantComms.ElectionResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getSendGreetingsMessageMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              plant.grpc.PlantComms.GreetingsMessage,
              plant.grpc.PlantComms.GreetingsResponse>(
                service, METHODID_SEND_GREETINGS_MESSAGE)))
        .addMethod(
          getSendElectionMessageMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              plant.grpc.PlantComms.ElectionMessage,
              plant.grpc.PlantComms.ElectionResponse>(
                service, METHODID_SEND_ELECTION_MESSAGE)))
        .build();
  }

  private static abstract class PlantCommunicationBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    PlantCommunicationBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return plant.grpc.PlantComms.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("PlantCommunication");
    }
  }

  private static final class PlantCommunicationFileDescriptorSupplier
      extends PlantCommunicationBaseDescriptorSupplier {
    PlantCommunicationFileDescriptorSupplier() {}
  }

  private static final class PlantCommunicationMethodDescriptorSupplier
      extends PlantCommunicationBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    PlantCommunicationMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (PlantCommunicationGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new PlantCommunicationFileDescriptorSupplier())
              .addMethod(getSendGreetingsMessageMethod())
              .addMethod(getSendElectionMessageMethod())
              .build();
        }
      }
    }
    return result;
  }
}
