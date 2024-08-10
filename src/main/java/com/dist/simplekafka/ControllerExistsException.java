package com.dist.simplekafka;

public class ControllerExistsException extends RuntimeException {
    int controllerId;

    public ControllerExistsException(int controllerId) {
        this.controllerId = controllerId;
    }

    public int getControllerId() {
        return controllerId;
    }
}
