package com.alibaba.hologres.client.copy.in;

import com.alibaba.hologres.client.copy.in.arrow.AbstractArrowWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@link CopyInStageWrapper} 的 dry-run 实现：不建立真实数据库连接，将数据以 Arrow 格式保存在内存中，便于单元测试验证写入内容与格式.
 *
 * <p>使用示例：
 *
 * <pre>{@code
 * DryRunCopyInStageWrapper<Record> wrapper =
 *         new DryRunCopyInStageWrapper<>("my_stage", "my_prefix", arrowWriter, 64 * 1024 * 1024);
 * wrapper.putRecord(record1);
 * wrapper.flush();
 * Map<String, byte[]> files = wrapper.getDryRunFiles();
 * // 验证 files 中的内容
 * }</pre>
 */
public class DryRunCopyInStageWrapper<RECORD> extends CopyInStageWrapper<RECORD> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DryRunCopyInStageWrapper.class);

    /** 按写入顺序存储：key=文件名, value=Arrow 字节内容. */
    private final LinkedHashMap<String, byte[]> dryRunFiles = new LinkedHashMap<>();

    /**
     * 构造 dry-run 实例.
     *
     * @param stageName stage 名称（仅用于构造文件名，不真正连接数据库）
     * @param filePrefix 文件前缀，用于构造虚拟文件名
     * @param arrowWriter Arrow 格式写入器
     * @param fileSizeLimit 单个文件大小上限（字节），超过后触发一次内存写入
     */
    public DryRunCopyInStageWrapper(
            String stageName,
            String filePrefix,
            AbstractArrowWriter<RECORD> arrowWriter,
            int fileSizeLimit) {
        super(null, stageName, filePrefix, arrowWriter, fileSizeLimit);
    }

    /**
     * 构造 dry-run 实例，使用默认 fileSizeLimit（64 MB）.
     *
     * @param stageName stage 名称
     * @param filePrefix 文件前缀
     * @param arrowWriter Arrow 格式写入器
     */
    public DryRunCopyInStageWrapper(
            String stageName, String filePrefix, AbstractArrowWriter<RECORD> arrowWriter) {
        this(stageName, filePrefix, arrowWriter, 64 * 1024 * 1024);
    }

    /** dry-run 模式不连接数据库，跳过压缩版本校验. */
    @Override
    void checkCompressionCapability() {
        // no-op: dry-run 只用于本地验证 Arrow 格式，不依赖实例版本
    }

    /** 将数据写入内存，文件名格式与生产模式一致（{prefix}_{index}.arrow），不建立数据库连接. */
    @Override
    public void copyInStageFromBytes(byte[] data) throws IOException {
        if (data.length == 0) {
            return;
        }
        String fileName = String.format("%s_%d.arrow", filePrefix, fileIndex++);
        LOGGER.info("[dry-run] copy in stage file({}) size={} bytes", fileName, data.length);
        dryRunFiles.put(fileName, data);
    }

    /**
     * 返回所有已写入的文件数据，key 为文件名，value 为 Arrow 字节内容，按写入顺序排列.
     *
     * @return 只读的文件名到字节内容的有序映射
     */
    public Map<String, byte[]> getDryRunFiles() {
        return Collections.unmodifiableMap(dryRunFiles);
    }
}
