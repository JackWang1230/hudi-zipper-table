package cn.uniodnrug.zipper.table.model;


import java.util.List;

/**
 * @author RWang
 * @Date 2022/1/11
 */

public class AtInfos {
    private List<String> atMobiles;
    private List<String> atUserIds;

    private boolean isAtAll;

    public List<String> getAtMobiles() {
        return atMobiles;
    }

    public void setAtMobiles(List<String> atMobiles) {
        this.atMobiles = atMobiles;
    }

    public List<String> getAtUserIds() {
        return atUserIds;
    }

    public void setAtUserIds(List<String> atUserIds) {
        this.atUserIds = atUserIds;
    }

    public boolean getisAtAll() {
        return isAtAll;
    }

    public void setisAtAll(boolean isAtAll) {
        this.isAtAll = isAtAll;
    }


}
