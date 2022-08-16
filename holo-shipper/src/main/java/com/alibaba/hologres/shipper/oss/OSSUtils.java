package com.alibaba.hologres.shipper.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.PutObjectRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class OSSUtils {
    public static final Logger LOGGER = LoggerFactory.getLogger(OSSDB.class);

    public static String readWholeFile(OSS ossClient, String bucketName, String filePath) {
        OSSObject ossObject = ossClient.getObject(bucketName, filePath);
        StringBuilder sb = new StringBuilder();
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(ossObject.getObjectContent()))) {
            while (true) {
                String line = reader.readLine();
                if (line == null) break;
                sb.append(line);
                sb.append('\n');
            }
        } catch (IOException e) {
            LOGGER.error("Failed reading file at " + filePath, e);
        }
        return sb.toString();
    }

    public static void writeStringToFile(OSS ossClient, String bucketName, String filePath, String content) {
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, filePath, new ByteArrayInputStream(content.getBytes()));
        // 如果需要上传时设置存储类型和访问权限，请参考以下示例代码。
        // ObjectMetadata metadata = new ObjectMetadata();
        // metadata.setHeader(OSSHeaders.OSS_STORAGE_CLASS, StorageClass.Standard.toString());
        // metadata.setObjectAcl(CannedAccessControlList.Private);
        // putObjectRequest.setMetadata(metadata);
        ossClient.putObject(putObjectRequest);
    }
}
