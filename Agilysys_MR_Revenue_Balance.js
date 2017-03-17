/**
 * Created by yic on 2/16/17.
 */

/**
 * @NApiVersion 2.x
 * @NScriptType MapReduceScript
 */

define(['N/search', 'N/record','N/runtime', 'N/error','./AGYS_Lib'], function(search, record, runtime, error, AGYS) {
    var exports = {};
    var scriptObj = runtime.getCurrentScript();
    var DEBUG = true;

    var buildBalanceObj = function(salesOrderId, searchId) {
        // load Analyzing Search for salesOrderId
        var analyzeBalance = search.load({
            id: searchId
        });

        if(DEBUG) {
            log.audit({
                title: 'Order ID',
                details: salesOrderId
            });
        }

        var idFilter = search.createFilter({
            name: 'internalidnumber',
            operator: search.Operator.EQUALTO,
            values: salesOrderId
        });

        analyzeBalance.filters.push(idFilter);

        var searchResults = analyzeBalance.run().getRange({
            start: 0,
            end: 200
        });

        if(searchResults.length > 0) {
            // intializing temporary object
            var tempObj = {
                deferred: 0,
                unearned: 0,
                unbilled: 0,
                balance: 0,
                revcom_status: [],
                invoice_status: [],
                so_status: []
            };

            return AGYS.calculateBalance(searchResults, tempObj);

        }
        return null;

    };

    var getInputData = function getInputData() {
        const SEARCH_TO_LOAD = scriptObj.getParameter({ name: 'custscript_ag_unbalanced_ids' });
        const SEARCH_TO_ANALYZE = scriptObj.getParameter({ name: 'custscript_ag_balance_analysis_search_id'});

        // Deploye IDs are different in Prod and Sandbox
        const DEPLOY_SCRIPT_ID = runtime.envType == "SANDBOX" ? '8326' : '8669';

        log.audit({
            title: "Search IDs",
            details: "Loaded Search IDS : " + SEARCH_TO_LOAD + ", " + SEARCH_TO_ANALYZE +"\n"
            + "Env: " + JSON.stringify(runtime.envType)
        });

        record.submitFields({
            type: record.Type.SCRIPT_DEPLOYMENT,
            id: DEPLOY_SCRIPT_ID,
            values: {
                custscript_ag_last_run: AGYS.getCurrentDate()
            }
        });

        // Load and return Search Object
        return search.load({
            id: SEARCH_TO_LOAD
        });

        //buildBalanceObj(test.id, SEARCH_TO_ANALYZE);

    };

    var map = function map(context) {
        const SEARCH_TO_ANALYZE = scriptObj.getParameter({ name: 'custscript_ag_balance_analysis_search_id'});
        if(DEBUG){
            log.debug({
                title: "Search Result in map",
                details: context.value
            });
        }

        var searchResult = JSON.parse(context.value);
        var salesOrderId = searchResult.id;

        // get search result in array
        var searchObj = buildBalanceObj(salesOrderId, SEARCH_TO_ANALYZE);

        if(DEBUG){
            log.audit({
                title: "Map Balance Object after search",
                details: JSON.stringify(searchObj)
            });
        }

        // Pair Sales Order ID and Search Result and write to context object
        context.write(salesOrderId, searchObj);
    };

    var reduce = function reduce(context) {
        context.write(context.key, JSON.parse(context.values));
    };

    var summarize = function summarize(summary) {
        AGYS.createRevenueBalance(summary);
        AGYS.handleErrorIfAny(summary);
    };

    exports.config = {
        exitOnError: false
    };
    exports.getInputData = getInputData;
    exports.map = map;
    exports.reduce = reduce;
    exports.summarize = summarize;

    return exports;
});
