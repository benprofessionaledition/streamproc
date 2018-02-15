package streamproc;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class SPSEvent {

    private String device;
    private String sev;
    private String title;
    private String country;
    private long time;

    public SPSEvent() {}

    @JsonIgnore
    public SPSEvent(SPSEvent other) {
        this.device = other.device;
        this.sev = other.sev;
        this.title = other.title;
        this.country = other.country;
        this.time = other.time;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getSev() {
        return sev;
    }

    public void setSev(String sev) {
        this.sev = sev;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override public String toString() {
        final StringBuilder sb = new StringBuilder("SPSEvent{");
        sb.append("device='").append(device).append('\'');
        sb.append(", sev='").append(sev).append('\'');
        sb.append(", title='").append(title).append('\'');
        sb.append(", country='").append(country).append('\'');
        sb.append(", time=").append(time);
        sb.append('}');
        return sb.toString();
    }
}
