package ClientHelpers;

import io.grpc.dao.TableResponse;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;

public class ResponseStreamObserverImpl implements StreamObserver<TableResponse> {

    private CountDownLatch finishLatch=null;
    boolean toContinue = false;

    public ResponseStreamObserverImpl(CountDownLatch latch){
        this.finishLatch = latch;
    }

    @Override
    public void onNext(TableResponse note) {
        System.out.println("服务端写回: " + note.getMesg());
        toContinue = true;
    }

    @Override public void onError(Throwable t) {
        t.printStackTrace();
        System.out.println("RouteChat Failed:");
        finishLatch.countDown();
    }

    @Override public void onCompleted() {
        System.out.println("Finished RouteChat");
        finishLatch.countDown();
    }

    public boolean toContinue(){
        if (toContinue){
            toContinue = false;
            return  true;
        }else {
            return false;
        }

    }
}
