package dao;


import bean.AdBlacklist;

import java.util.List;

/**
 * FileName: IAdBlacklistDAO
 * Author:   hadoop
 * Email:    3165845957@qq.com
 * Date:     19-4-3 下午3:39
 * Description:
 * 广告黑名单DAO接口
 */
public interface IAdBlacklistDAO {
    /**
     * 批量插入广告黑名单用户
     * @param adBlacklists
     */
     void insertBatch(List<AdBlacklist> adBlacklists);

    /**
     * 查找所有广告黑名单用户
     * @return
     */
    List<AdBlacklist> findAll();
}