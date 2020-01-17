package dao.impl;

import bean.AdBlacklist;
import dao.IAdBlacklistDAO;
import utils.JDBCHelper;

import java.util.List;
import java.sql.ResultSet;
import java.util.ArrayList;


/**
 * FileName: AdBlacklistDAOImpl
 * Author:   hadoop
 * Email:    3165845957@qq.com
 * Date:     19-4-3 下午3:43
 * Description:
 */
public class AdBlacklistDAOImpl implements IAdBlacklistDAO {
    /**
     * 批量插入广告黑名单用户
     * @param adBlacklists
     */
    @Override
    public void insertBatch(List<AdBlacklist> adBlacklists) {
        String insertSQL = "INSERT INTO ad_blacklist VALUES(?)";
        List<Object[]> paramLiist = new ArrayList<Object[]>();

        for (AdBlacklist adBlacklist : adBlacklists){
            Object[] params = new Object[]{adBlacklist.getUserid()};
            paramLiist.add(params);
        }

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeBatch(insertSQL,paramLiist);
    }

    /**
     * 查询所有的广告黑名单用户
     * @return
     */
    @Override
    public List<AdBlacklist> findAll() {
        String selectSQL = "SELECT * FROM ad_blacklist";

        final List<AdBlacklist> adBlacklists = new ArrayList<AdBlacklist>();
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeQuery(selectSQL, null, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while (rs.next()){
                    long userid = rs.getLong(1);
                    //long userid = Long.valueOf(String.valueOf(rs.getInt(1));
                    AdBlacklist adBlacklist = new AdBlacklist();
                    adBlacklist.setUserid(userid);

                    adBlacklists.add(adBlacklist);
                }
            }
        });
        return adBlacklists;
    }
}
