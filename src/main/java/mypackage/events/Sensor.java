package mypackage.events;

public class Sensor {
    private int deviceId;
    private double temperature;
    private long timestamp;

    public Sensor(int deviceId, double temperature, long timestamp)
    {
        this.deviceId = deviceId;
        this.temperature = temperature;
        this.timestamp = timestamp;
    }

    public int getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(int deviceId) {
        this.deviceId = deviceId;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "device id: " + this.deviceId + ", temperature: " + this.temperature;
    }
}
