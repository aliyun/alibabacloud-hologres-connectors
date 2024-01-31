package com.alibaba.hologres;

import com.aliyun.teaopenapi.models.Config;
import com.aliyun.teautil.models.RuntimeOptions;
import com.aliyun.ververica20220718.Client;
import com.aliyun.ververica20220718.models.Deployment;
import com.aliyun.ververica20220718.models.GetJobHeaders;
import com.aliyun.ververica20220718.models.GetJobResponse;
import com.aliyun.ververica20220718.models.Job;
import com.aliyun.ververica20220718.models.ListDeploymentsHeaders;
import com.aliyun.ververica20220718.models.ListDeploymentsRequest;
import com.aliyun.ververica20220718.models.ListDeploymentsResponse;
import com.aliyun.ververica20220718.models.ListJobsHeaders;
import com.aliyun.ververica20220718.models.ListJobsRequest;
import com.aliyun.ververica20220718.models.ListJobsResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FindIncompatibleFlinkJobs {
    static List<IncompatibleResult> incompatibleResults = new ArrayList<>();

    public static void main(String[] args) throws Exception {

        String endpoint = REGION.get(args[0]);
        if (endpoint == null) {
            System.out.println("Invalid region");
            return;
        }
        String[] extractedResults = extractWorkspaceAndNamespace(args[1]);
        if (extractedResults == null) {
            System.out.println("Invalid url");
            return;
        }
        String workspaceId = extractedResults[0];
        String namespace = extractedResults[1];

        Config config = new com.aliyun.teaopenapi.models.Config();
        config.setEndpoint(endpoint);
        config.setAccessKeyId(args[2]);
        config.setAccessKeySecret(args[3]);

        // 检查binlog的可能不兼容作业，还是检查rpc模式的可能不兼容作业
        boolean binlog = "binlog".equals(args[4]);

        Client client = new Client(config);

        // 获取已部署作业列表。
        ListDeploymentsRequest listDeploymentsRequest = new ListDeploymentsRequest();
        listDeploymentsRequest.setPageIndex(1);
        listDeploymentsRequest.setPageSize(100);
        ListDeploymentsHeaders listDeploymentsHeaders = new ListDeploymentsHeaders();
        listDeploymentsHeaders.setWorkspace(workspaceId);
        ListDeploymentsResponse listDeploymentsResponse =
                client.listDeploymentsWithOptions(namespace, listDeploymentsRequest, listDeploymentsHeaders, new RuntimeOptions());

        for (Deployment deployment : listDeploymentsResponse.getBody().getData()) {
            String deploymentName = deployment.getName();
            String deploymentVersion = deployment.getEngineVersion();

            // 只看运行中的作业
            if (deployment.getJobSummary().getRunning() != 1) {
                System.out.println(deploymentName + " not a running job, skip");
                continue;
            }
            // 只看sql作业， jar作业需要用户自行确认
            if (!"sqlScript".equalsIgnoreCase(deployment.getArtifact().getKind())) {
                System.out.println(deploymentName + " not sql job, skip");
                continue;
            }
            // vvr-8.0.5-flink-1.17 版本之后，会有自动切换源表消费binlog为jdbc模式的逻辑
            if (deploymentVersion.compareTo("vvr-8.0.5-flink-1.17") >= 0) {
                System.out.println(deploymentName + " check binlog: version is " + deploymentVersion + ", skip");
                continue;
            }
            // vvr-6.0.7-flink-1.15 版本之后，会有自动切换rpc模式为jdbc模式的逻辑
            if (!binlog && deploymentVersion.compareTo("vvr-6.0.7-flink-1.17") >= 0) {
                System.out.println(deploymentName + " check rpc: version is " + deploymentVersion + ", skip");
                continue;
            }
            // 获取作业实例列表。
            ListJobsRequest listJobsRequest = new ListJobsRequest();
            listJobsRequest.setDeploymentId(deployment.getDeploymentId());
            listJobsRequest.setPageIndex(1);
            listJobsRequest.setPageSize(100);
            ListJobsHeaders listJobsHeaders = new ListJobsHeaders();
            listJobsHeaders.setWorkspace(workspaceId);
            ListJobsResponse listJobsResponse =
                    client.listJobsWithOptions(namespace, listJobsRequest, listJobsHeaders, new RuntimeOptions());

            for (int i = 0; i < listJobsResponse.getBody().getData().size(); i++) {
                Job job = listJobsResponse.getBody().getData().get(i);
                // 只看运行中的作业
                if (!"RUNNING".equalsIgnoreCase(job.getStatus().getCurrentJobStatus())) {
                    continue;
                }

                // 获取作业实例。
                GetJobHeaders getJobHeaders = new GetJobHeaders();
                getJobHeaders.setWorkspace(workspaceId);
                GetJobResponse getJobResponse =
                        client.getJobWithOptions(namespace, job.jobId, getJobHeaders, new RuntimeOptions());

                String sql = getJobResponse.getBody().getData().getArtifact().getSqlArtifact().sqlScript;
                if (binlog) {
                    checkBinlog(sql, deploymentName, deploymentVersion);
                } else {
                    checkRpc(sql, deploymentName, deploymentVersion);
                }
            }
        }

        if (binlog) {
            System.out.println("\n\n--------------------以下是版本还小于8.0.5，也没有设置sdkmode = jdbc的hologres binlog 源表--------------------");
        } else {
            System.out.println("\n\n--------------------以下是版本还小于6.0.7，也没有设置sdkmode = jdbc的hologres rpc 维表/结果表----------------");
        }
        System.out.println("-------------------------------------------------------------------------------------------------------------");
        System.out.println("deploymentName                       version                              tableName");
        for (IncompatibleResult badResult : incompatibleResults) {
            System.out.println(badResult.deploymentName + " " + badResult.deploymentVersion + " " + badResult.tableName);
        }
    }

    public static void checkBinlog(String sql, String deploymentName, String deploymentVersion) {
        if (sql.contains("hologres") && sql.contains("binlog")) {
            Set<Map<String, String>> allHologresTables = extractParameters(sql);
            for (Map<String, String> table : allHologresTables) {
                String tableName = table.get("tablename");
                // 只看binlog源表
                if (table.get("binlog") == null) {
                    System.out.println(tableName + " in " + deploymentName + " not a hologres binlog table, skip");
                    continue;
                }
                // 手动设置了 sdkMode = jdbc, okk
                if ("jdbc".equalsIgnoreCase(table.get("sdkmode"))) {
                    System.out.println(tableName + " in " + deploymentName + " is a hologres binlog table and have set sdkMode = jdbc, skip");
                    continue;
                }
                // 其他的都是有问题的
                incompatibleResults.add(new IncompatibleResult(deploymentName, deploymentVersion, tableName));
            }
        } else {
            System.out.println(deploymentName + " not a hologres job, skip");
        }
    }

    public static void checkRpc(String sql, String deploymentName, String deploymentVersion) {
        if (sql.contains("hologres") && sql.contains("rpc")) {
            Set<Map<String, String>> allHologresTables = extractParameters(sql);
            for (Map<String, String> table : allHologresTables) {
                String tableName = table.get("tablename");
                // 只看开启了rpc的表
                if (table.get("userpcmode") == null && !"rpc".equals(table.get("sdkmode"))) {
                    System.out.println(tableName + " in " + deploymentName + " not a hologres rpc table, skip");
                    continue;
                }
                // 手动设置了 sdkMode = jdbc, okk
                if ("jdbc".equalsIgnoreCase(table.get("sdkmode"))) {
                    System.out.println(tableName + " in " + deploymentName + " is a hologres binlog table and have set sdkMode = jdbc, skip");
                    continue;
                }
                // 其他的都是有问题的
                incompatibleResults.add(new IncompatibleResult(deploymentName, deploymentVersion, tableName));
            }
        } else {
            System.out.println(deploymentName + " not a hologres job, skip");
        }
    }

    public static HashMap<String, String> REGION = new HashMap<String, String>() {
        {
            put("北京", "ververica.cn-beijing.aliyuncs.com");
            put("上海", "ververica.cn-shanghai.aliyuncs.com");
            put("杭州", "ververica.cn-hangzhou.aliyuncs.com");
            put("深圳", "ververica.cn-shenzhen.aliyuncs.com");
            put("张家口", "ververica.cn-zhangjiakou.aliyuncs.com");
            put("香港", "ververica.cn-hongkong.aliyuncs.com");
            put("新加坡", "ververica.ap-southeast-1.aliyuncs.com");
            put("德国", "ververica.eu-central-1.aliyuncs.com");
            put("英国", "ververica.eu-west-1.aliyuncs.com");
            put("印度尼西亚", "ververica.ap-southeast-5.aliyuncs.com");
            put("马来西亚", "ververica.ap-southeast-3.aliyuncs.com");
            put("美国", "ververica.us-west-1.aliyuncs.com");
            put("上海金融云", "ververica.cn-shanghai-finance-1.aliyuncs.com");
        }
    };

    public static String[] extractWorkspaceAndNamespace(String url) {
        // 正则表达式匹配 'workspaces' 后的字符串和 'namespaces' 后的字符串
        String pattern = ".*/workspaces/([^/]+)/namespaces/([^/]+)/.*";

        // 编译正则表达式
        Pattern compiledPattern = Pattern.compile(pattern);
        // 将正则表达式应用到字符串上
        Matcher matcher = compiledPattern.matcher(url);

        // 检查是否找到匹配
        if (matcher.find()) {
            // 将匹配到的结果作为数组返回
            return new String[]{matcher.group(1), matcher.group(2)};
        }
        return null; // 如果没有匹配，返回null
    }

    public static Set<Map<String, String>> extractParameters(String input) {
        Set<Map<String, String>> allHologresTables = new HashSet<>();

        // 匹配包含 'with' 的完整块，并捕获括号中的内容，忽略大小写
        String withBlockRegex = "(?i)with\\s*\\((.*?)\\)";
        Pattern withBlockPattern = Pattern.compile(withBlockRegex, Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
        Matcher withBlockMatcher = withBlockPattern.matcher(input);

        while (withBlockMatcher.find()) {
            // 从 'with' 块中捕获的内容
            String withBlockContent = withBlockMatcher.group(1);

            // 匹配所有键值对
            String kvRegex = "'(.*?)'\\s*=\\s*'([^']*)'";
            Pattern kvPattern = Pattern.compile(kvRegex, Pattern.CASE_INSENSITIVE);
            Matcher kvMatcher = kvPattern.matcher(withBlockContent);

            Map<String, String> parameters = new HashMap<>();

            while (kvMatcher.find()) {
                String key = kvMatcher.group(1).toLowerCase();
                String value = kvMatcher.group(2);
                parameters.put(key, value);
            }

            // 如果包含目标 connector，则将其参数映射添加到结果中
            if ("hologres".equalsIgnoreCase(parameters.get("connector"))) {
                allHologresTables.add(parameters);
            }
        }
        return allHologresTables;
    }
}
