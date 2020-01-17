package bean;

import java.io.Serializable;

/**
 * FileName: AdBlacklist
 * Author:   hadoop
 * Email:    3165845957@qq.com
 * Date:     19-4-3 下午3:38
 * Description:
 * 用户黑名单实体类
 */
public class AdBlacklist implements Serializable{
    private Long userid ;

    public Long getUserid() {
        return userid;
    }

    public void setUserid(Long userid) {
        this.userid = userid;
    }

    @Override
    public String toString() {
        return "AdBlacklist{" +
                "userid=" + userid +
                '}';
    }
}
