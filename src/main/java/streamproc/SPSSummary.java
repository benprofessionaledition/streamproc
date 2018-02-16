package streamproc;

/**
 * An aggregation of "play" events
 */
public class SPSSummary {

    /**
     * The underlying hardware device
     */
    private String device;

    /**
     * The play events per second for this device, title and country
     */
    private int sps;

    /**
     * The title being played
     */
    private String title;

    /**
     * The originating country
     */
    private String country;

    public SPSSummary() {}

    public SPSSummary(String device, int sps, String title, String country) {
        this.device = device;
        this.sps = sps;
        this.title = title;
        this.country = country;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public int getSps() {
        return sps;
    }

    public void setSps(int sps) {
        this.sps = sps;
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

    /**
     * {@inheritDoc}
     */
    @Override public String toString() {
        final StringBuilder sb = new StringBuilder("SPSSummary{");
        sb.append("device='").append(device).append('\'');
        sb.append(", sps=").append(sps);
        sb.append(", title='").append(title).append('\'');
        sb.append(", country='").append(country).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
