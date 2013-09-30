package com.netflix.ice.basic;

import com.google.common.collect.Lists;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.processor.ProcessorConfig;
import com.netflix.ice.reader.ReaderConfig;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BasicResourceService extends ResourceService {
    protected Logger logger = LoggerFactory.getLogger(BasicResourceService.class);
    private ProcessorConfig processorConfig;
    public static final String MEDISTRANO_TESTS = "medistrano_tests";

    @Override
    public void init() {
        logger.info("Retrieving processor config for custom BasicResourceService");
        processorConfig = ProcessorConfig.getInstance();
    }

    @Override
    public String getResource(Account account, Region region, Product product, String resourceId, String[] lineItem, long millisStart) {
        List<String> header = processorConfig.lineItemProcessor.getHeader();

        // Use bucket name for S3 resources
        if (product == Product.s3) {
            return (resourceId).toLowerCase();
        }

        String productName = "";
        String result = "";
        int productIndex = header.indexOf("user:Product");

        if (productIndex > 0 && lineItem.length > productIndex && !StringUtils.isEmpty(lineItem[productIndex])) {
            productName = lineItem[productIndex];

            if (productName.contains("ztestz")) {
                return MEDISTRANO_TESTS;
            } else {
                for (String tag: processorConfig.customTags) {
                    int index = header.indexOf(tag);
                    if (index > 0 && lineItem.length > index && !StringUtils.isEmpty(lineItem[index]))
                        result = StringUtils.isEmpty(result) ? lineItem[index] : result + "_" + lineItem[index];
                }
            }
        }

        return StringUtils.isEmpty(result) ? product.name : result;
    }

    @Override
    public List<List<Product>> getProductsWithResources() {
        List<List<Product>> result = Lists.newArrayList();
        for (Product product: ReaderConfig.getInstance().productService.getProducts()) {
            result.add(Lists.<Product>newArrayList(product));
        }
        return result;
    }

    @Override
    public void commit() {

    }
}
