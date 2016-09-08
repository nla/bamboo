package bamboo.pandas;

public class PeriodRestr {
    private int periodMultiplier;
    private int periodTypeId;
    private long titleId;

    public PeriodRestr() {}

    public long getTitleId() {
        return titleId;
    }

    public void setTitleId(long titleId) {
        this.titleId = titleId;
    }

    public int getPeriodMultiplier() {
        return periodMultiplier;
    }

    public void setPeriodMultiplier(int periodMultiplier) {
        this.periodMultiplier = periodMultiplier;
    }

    public int getPeriodTypeId() {
        return periodTypeId;
    }

    public void setPeriodTypeId(int periodTypeId) {
        this.periodTypeId = periodTypeId;
    }

    public long getSecondsSinceCapture() {
        switch (periodTypeId) {
            case 0: // forever
                return Integer.MAX_VALUE;
            case 1: // days
                return periodMultiplier * 60 * 60 * 24;
            case 2: // weeks
                return periodMultiplier * 60 * 60 * 24 * 7;
            case 3: // months
                return periodMultiplier * 60 * 60 * 24 * 31;
            case 4: // years
                return periodMultiplier * 60 * 60 * 24 * 365;
            default:
                throw new IllegalArgumentException("unknown period_type_id " + periodTypeId);
        }
    }
}
