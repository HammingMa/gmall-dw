package com.mzh.gmall.dw.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class EventLogUDTF extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //检查输入参数
        List<? extends StructField> allStructFieldRefs = argOIs.getAllStructFieldRefs();
        if (allStructFieldRefs.size() != 1) {
            throw new UDFArgumentException("输入参数只能为一个");
        }

        if (!"string".equals(allStructFieldRefs.get(0).getFieldObjectInspector().getTypeName())) {
            throw new UDFArgumentException("输入参数为string类型");
        }

        //返回输出参数的检查器
        List<String> fieldNames = new ArrayList<String>();
        fieldNames.add("event_type");
        fieldNames.add("event_json");

        List<ObjectInspector> fieldOIS = new ArrayList<ObjectInspector>();
        fieldOIS.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIS.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIS);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        if (objects == null || objects.length == 0 || objects[0] == null) {
            return;
        }
        String input = objects[0].toString();

        if (StringUtils.isBlank(input)) {
            return;
        }

        try {
            JSONArray jsonArray = new JSONArray(input);
            if (jsonArray == null) {
                return;
            }

            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject event = jsonArray.getJSONObject(i);
                String type = event.getString("en");
                String eventStr = event.toString();

                String[] result = new String[2];
                result[0] = type;
                result[1] = eventStr;

                forward(result);
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
