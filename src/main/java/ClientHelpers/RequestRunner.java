package ClientHelpers;

import io.grpc.Status;
import io.grpc.dao.InfoRequest;
import io.grpc.dao.TableResponse;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class RequestRunner implements Runnable {
    private List<InfoRequest> infoList = null;

    public RequestRunner(List<InfoRequest> infoList){
        this.infoList = infoList;
    }

    @Override
    public void run() {

    }
}
