package streamproc;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Objects;

/**
 * POJO/Bean class that encapsulates a single play event
 */
public class SPSEvent {

    /**
     * The hardware device
     */
    private String device;

    /**
     * A status code
     */
    private String sev;

    /**
     * The title being played
     */
    private String title;

    /**
     * The originating timestamp
     */
    private String country;

    /**
     * UNIX epoch time
     */
    private long time;

    /**
     * Empty constructor explicitly declared for Jackson
     */
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

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SPSEvent spsEvent = (SPSEvent) o;
        return time == spsEvent.time &&
                Objects.equals(device, spsEvent.device) &&
                Objects.equals(sev, spsEvent.sev) &&
                Objects.equals(title, spsEvent.title) &&
                Objects.equals(country, spsEvent.country);
    }

    /**
     * {@inheritDoc}
     */
    @Override public int hashCode() {

        return Objects.hash(device, sev, title, country, time);
    }
}
