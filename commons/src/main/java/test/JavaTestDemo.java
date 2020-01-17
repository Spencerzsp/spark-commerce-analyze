package test;

import bean.AdBlacklist;
import conf.ConfigurationManager;
import constant.Constants;
import dao.impl.AdBlacklistDAOImpl;

import java.util.List;

public class JavaTestDemo {

    public static void main(String[] args) {

        String s = ConfigurationManager.config().getString(Constants.JDBC_DRIVER());
        System.out.println(s);

        AdBlacklistDAOImpl adBlacklistDAO = new AdBlacklistDAOImpl();
        List<AdBlacklist> all = adBlacklistDAO.findAll();

        for (AdBlacklist adBlacklist : all) {

            System.out.println(adBlacklist);
        }
    }
}
