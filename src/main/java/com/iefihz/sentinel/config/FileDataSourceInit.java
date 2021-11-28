package com.iefihz.sentinel.config;

import com.alibaba.csp.sentinel.command.handler.ModifyParamFlowRulesCommandHandler;
import com.alibaba.csp.sentinel.datasource.FileRefreshableDataSource;
import com.alibaba.csp.sentinel.datasource.FileWritableDataSource;
import com.alibaba.csp.sentinel.datasource.ReadableDataSource;
import com.alibaba.csp.sentinel.datasource.WritableDataSource;
import com.alibaba.csp.sentinel.init.InitFunc;
import com.alibaba.csp.sentinel.slots.block.authority.AuthorityRule;
import com.alibaba.csp.sentinel.slots.block.authority.AuthorityRuleManager;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRule;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRuleManager;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRuleManager;
import com.alibaba.csp.sentinel.slots.block.flow.param.ParamFlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.param.ParamFlowRuleManager;
import com.alibaba.csp.sentinel.slots.system.SystemRule;
import com.alibaba.csp.sentinel.slots.system.SystemRuleManager;
import com.alibaba.csp.sentinel.transport.util.WritableDataSourceRegistry;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

/**
 * 拉模式规则持久化到文件
 * 1.添加依赖：sentinel-datasource-extension
 * 2.在项目的 resources/META-INF/services 目录下创建文件，名为 com.alibaba.csp.sentinel.init.InitFunc ，
 *    内容为：当前类的全路径 {@link FileDataSourceInit}
 *
 * @author He Zhifei
 * @date 2020/11/15
 */
public class FileDataSourceInit implements InitFunc {

    private static final String SUB_DIR = "/sentinel-rules";
    // tips: 通过sentinel.ruleDir指定规则保存的根路径
    private static final String RULE_DIR = System.getProperty("sentinel.ruleDir") != null ? System.getProperty("sentinel.ruleDir") + SUB_DIR : SUB_DIR;
    private static final String FLOW_RULE_FILE = RULE_DIR + "/flow-rule.json";
    private static final String DEGRADE_RULE_FILE = RULE_DIR + "/degrade-rule.json";
    private static final String SYSTEM_RULE_FILE = RULE_DIR + "/system-rule.json";
    private static final String AUTHORITY_RULE_FILE = RULE_DIR + "/authority-rule.json";
    private static final String PARAM_FLOW_RULE_FILE = RULE_DIR + "/param-flow-rule.json";

    @Override
    public void init() throws Exception {
        this.mkdirIfNotExits(RULE_DIR);
        this.createFilesIfNotExits(FLOW_RULE_FILE, DEGRADE_RULE_FILE, SYSTEM_RULE_FILE,
                AUTHORITY_RULE_FILE, PARAM_FLOW_RULE_FILE);

        // 分别处理流控、降级、系统、授权、热点参数规则
        handleFlowRule(FLOW_RULE_FILE);
        handleDegradeRule(DEGRADE_RULE_FILE);
        handleSystemRule(SYSTEM_RULE_FILE);
        handleAuthorityRule(AUTHORITY_RULE_FILE);
        handleParamFlowRule(PARAM_FLOW_RULE_FILE);
    }

    /**
     * 处理流控规则
     * @param flowRuleFile
     * @throws FileNotFoundException
     */
    private void handleFlowRule(String flowRuleFile) throws FileNotFoundException {
        ReadableDataSource<String, List<FlowRule>> flowRuleRDS = new FileRefreshableDataSource<>(flowRuleFile,
                source -> JSON.parseObject(source, new TypeReference<List<FlowRule>>() {}));
        // 将可读数据源注册至FlowRuleManager，当规则文件发生变化时，就会更新规则到内存
        FlowRuleManager.register2Property(flowRuleRDS.getProperty());

        WritableDataSource<List<FlowRule>> flowRuleWDS = new FileWritableDataSource<>(flowRuleFile, this::encodeJson);
        // 将可写数据源注册至transport模块的WritableDataSourceRegistry中，收到控制台推送的规则时，Sentinel会先更新到内存，然后将规则写入到文件中
        WritableDataSourceRegistry.registerFlowDataSource(flowRuleWDS);
    }

    /**
     * 处理降级规则
     * @param degradeRuleFile
     * @throws FileNotFoundException
     */
    private void handleDegradeRule(String degradeRuleFile) throws FileNotFoundException {
        ReadableDataSource<String, List<DegradeRule>> degradeRuleRDS = new FileRefreshableDataSource<>(degradeRuleFile,
                source -> JSON.parseObject(source, new TypeReference<List<DegradeRule>>() {}));
        DegradeRuleManager.register2Property(degradeRuleRDS.getProperty());
        WritableDataSource<List<DegradeRule>> degradeRuleWDS = new FileWritableDataSource<>(degradeRuleFile, this::encodeJson);
        WritableDataSourceRegistry.registerDegradeDataSource(degradeRuleWDS);
    }

    /**
     * 处理系统规则
     * @param systemRuleFile
     */
    private void handleSystemRule(String systemRuleFile) throws FileNotFoundException {
        ReadableDataSource<String, List<SystemRule>> systemRuleRDS = new FileRefreshableDataSource<>(systemRuleFile,
                source -> JSON.parseObject(source, new TypeReference<List<SystemRule>>() {}));
        SystemRuleManager.register2Property(systemRuleRDS.getProperty());
        WritableDataSource<List<SystemRule>> systemRuleWDS = new FileWritableDataSource<>(systemRuleFile, this::encodeJson);
        WritableDataSourceRegistry.registerSystemDataSource(systemRuleWDS);
    }

    /**
     * 处理授权规则
     * @param authorityRuleFile
     * @throws FileNotFoundException
     */
    private void handleAuthorityRule(String authorityRuleFile) throws FileNotFoundException {
        ReadableDataSource<String, List<AuthorityRule>> authorityRuleRDS = new FileRefreshableDataSource<>(authorityRuleFile,
                source -> JSON.parseObject(source, new TypeReference<List<AuthorityRule>>() {}));
        AuthorityRuleManager.register2Property(authorityRuleRDS.getProperty());
        WritableDataSource<List<AuthorityRule>> authorityRuleWDS = new FileWritableDataSource<>(authorityRuleFile, this::encodeJson);
        WritableDataSourceRegistry.registerAuthorityDataSource(authorityRuleWDS);
    }

    /**
     * 处理热点参数规则
     * @param paramFlowRuleFile
     * @throws FileNotFoundException
     */
    private void handleParamFlowRule(String paramFlowRuleFile) throws FileNotFoundException {
        ReadableDataSource<String, List<ParamFlowRule>> paramFlowRuleRDS = new FileRefreshableDataSource<>(paramFlowRuleFile,
                source -> JSON.parseObject(source, new TypeReference<List<ParamFlowRule>>() {}));
        ParamFlowRuleManager.register2Property(paramFlowRuleRDS.getProperty());
        WritableDataSource<List<ParamFlowRule>> paramFlowRuleWDS = new FileWritableDataSource<>(paramFlowRuleFile, this::encodeJson);
        ModifyParamFlowRulesCommandHandler.setWritableDataSource(paramFlowRuleWDS);
    }

    private void mkdirIfNotExits(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    private void createFilesIfNotExits(String... files) throws IOException {
        for (int i = 0; i < files.length; i ++) {
            File file = new File(files[i]);
            if (!file.exists()) {
                file.createNewFile();
            }
        }
    }

    private <T> String encodeJson(T t) {
        return JSON.toJSONString(t);
    }
}
