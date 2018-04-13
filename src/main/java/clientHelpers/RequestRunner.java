package clientHelpers;

import io.grpc.dao.InfoRequest;

import java.util.List;

public class RequestRunner implements Runnable {
    private List<InfoRequest> infoList = null;

    public RequestRunner(List<InfoRequest> infoList){
        this.infoList = infoList;
    }

    @Override
    public void run() {

    }
}
