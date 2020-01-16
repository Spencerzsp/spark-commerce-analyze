package utils;



/**
 * 校验工具类
 * @author Administrator
 *
 */
public class ValidUtils {

    /**
     * 校验数据中的指定字段，是否在指定范围内
     * @param data 数据
     * @param dataField 数据字段
     * @param parameter 参数
     * @param startParamField 起始参数字段
     * @param endParamField 结束参数字段
     * @return 校验结果
     */
    public static boolean between(String data, String dataField,
                                  String parameter, String startParamField, String endParamField) {
        String startParamFieldStr = StringUtils.getFieldFromConcatString(
                parameter, "\\|", startParamField);
        String endParamFieldStr = StringUtils.getFieldFromConcatString(
                parameter, "\\|", endParamField);
        if(startParamFieldStr == null || endParamFieldStr == null) {
            return true;
        }

        int startParamFieldValue = Integer.valueOf(startParamFieldStr);
        int endParamFieldValue = Integer.valueOf(endParamFieldStr);

        String dataFieldStr = StringUtils.getFieldFromConcatString(
                data, "\\|", dataField);
        if(dataFieldStr != null) {
            int dataFieldValue = Integer.valueOf(dataFieldStr);
            if(dataFieldValue >= startParamFieldValue &&
                    dataFieldValue <= endParamFieldValue) {
                return true;
            } else {
                return false;
            }
        }

        return false;
    }

    /**
     * 校验数据中的指定字段，是否有值与参数字段的值相同
     * @param data 数据"sessionid=63746095035e4c89a5b5bd0bd82e9988|" +
        "searchwords=华为手机,小龙虾,保温杯,卫生纸,苹果耳机,RedmiBook,MacBook,小米笔记本,吸尘器|" +
        "clickCategoryIds=49,67,31,42,19,95,22,27,86,24,9,12,69,30,20,30,96|" +
        "visitLength=3487|" +
        "stepLength=84|" +
        "startTime=2020-01-13 02:00:06|" +
        "age=18|" +
        "professional=professional87|" +
        "sex=female|" +
        "city=city94"
     * @param dataField 数据字段
     * @param parameter 限制条件的参数 "startAge=20|endAge=50|professionals=|cities=|sex=|keywords=|categoryIds="
     * @param paramField 限制条件的参数字段
     * @return 校验结果
     */
    public static boolean in(String data, String dataField,
                             String parameter, String paramField) {
        String paramFieldValue = StringUtils.getFieldFromConcatString(
                parameter, "\\|", paramField);
        if(paramFieldValue == null) {
            return true;
        }
        String[] paramFieldValueSplited = paramFieldValue.split(",");

        String dataFieldValue = StringUtils.getFieldFromConcatString(
                data, "\\|", dataField);
        if(dataFieldValue != null) {
            String[] dataFieldValueSplited = dataFieldValue.split(",");

            for(String singleDataFieldValue : dataFieldValueSplited) {
                for(String singleParamFieldValue : paramFieldValueSplited) {
                    if(singleDataFieldValue.equals(singleParamFieldValue)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * 校验数据中的指定字段，是否在指定范围内
     * @param data 数据
     * @param dataField 数据字段
     * @param parameter 参数
     * @param paramField 参数字段
     * @return 校验结果
     */
    public static boolean equal(String data, String dataField,
                                String parameter, String paramField) {
        String paramFieldValue = StringUtils.getFieldFromConcatString(
                parameter, "\\|", paramField);
        if(paramFieldValue == null) {
            return true;
        }

        String dataFieldValue = StringUtils.getFieldFromConcatString(
                data, "\\|", dataField);
        if(dataFieldValue != null) {
            if(dataFieldValue.equals(paramFieldValue)) {
                return true;
            }
        }

        return false;
    }

}