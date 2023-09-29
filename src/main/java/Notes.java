public class Notes {
    public int getI() {
        return i;
    }

    private int i;

    public int getTagId() {
        return tagId;
    }

    private int tagId;

    public String getTagName() {
        return tagName;
    }

    private String tagName;


    public Notes(int i, String tagName, int tagId) {
        this.i  = i;
        this.tagName = tagName;
        this.tagId = tagId;

    }


}
