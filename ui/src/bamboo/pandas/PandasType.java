package bamboo.pandas;

public enum PandasType {
    TITLE(0), SUBJECT(1), COLLECTION(2), ISSUE(3), ISSUE_GROUP(4), AGENCY(5);

    private final int id;

    PandasType(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }
}
