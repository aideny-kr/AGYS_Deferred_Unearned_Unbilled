/**
 * @NApiVersion 2.x
 * @NModuleScope Public
 */

define(['N/record', 'N/error', 'N/email', 'N/runtime', 'N/format'], function(record, error, email, runtime, format){

    /**
     * UTILS & CONST
     */

    const COLUMN_INDEX = {
        "SALES_ORDER_STATUS" : 0,
        "LINE_ID" : 1,
        "INVOICE_AMOUNT" : 2,
        "RECOGNIZED_AMOUNT": 3,
        "BALANCE": 4,
        "INVOICE_STATUS": 5,
        "REVCOM_STATUS": 6
    };

    var mergeAndGetUniqueStatus = function(arr1, arr2) {
        var newArr = arr1.concat(arr2);
        return newArr.filter(function(item, pos) {
           return newArr.indexOf(item) === pos;
        });
    };

    var getCurrentDate = function() {
        var parsedDateString = format.parse({
            value: new Date(),
            type: format.Type.DATE,
            timezone: format.Timezone.AMERICA_NEW_YORK
        });

        return format.format({
            value: parsedDateString,
            type: format.Type.DATE
        });
    }


    /**
     * EXPORTS and FUNCTIONS
     */
    var exports = {};
    var handleErrorIfAny = function(summary) {
        var inputSummary = summary.inputSummary;
        var mapSummary = summary.mapSummary;
        var reduceSummary = summary.reduceSummary;

        if(inputSummary.error) {
            var e = error.create({
                name: 'INPUT_STAGE_FAILED',
                message: inputSummary.error
            });

            handleErrorAndSendNotification(e, 'getInputData');
        }

        handleErrorInStage('map', mapSummary)

    };

    var handleErrorInStage = function(stage, summary) {
        var errorMsg = [];
        summary.errors.iterator().each(function(key, value) {
           var msg = 'Error during processing Sales Order ID : ' + key + '\n';
           errorMsg.push(msg);
           return true;
        });

        if(errorMsg.length > 0) {
            var e = error.create({
                name: 'CALCULATION_FAILED',
                message: JSON.stringify(errorMsg)
            });
            handleErrorAndSendNotification(e, stage);
        }
    };

    var handleErrorAndSendNotification = function(e, stage) {
        log.error('Stage: ' + stage + ' failed', e);

        var author = -5;
        var recipients = 'chan.yi@agilysys.com';
        var subject = 'Map/Reduce script ' + runtime.getCurrentScript().id + ' failed for stage: ' + stage;
        var body = 'An error occurred with the following information:\n' +
            'Error code: ' + e.name + '\n' +
            'Error msg: ' + e.message;

        email.send({
            author: author,
            recipients: recipients,
            subject: subject,
            body: body
        });
    };

    /**
     * function calculates deferred/unearned/unbilled and balance and return the calculated result
     * @param results from NetSuite search
     * @param initialzed object
     * @return {object}
     */

    var calculateBalance = function(results, obj) {
        results.forEach(function(result) {
            // get value for balance
            var balance = +result.getValue({
                name: result.columns[COLUMN_INDEX.BALANCE].name,
                summary: result.columns[COLUMN_INDEX.BALANCE].summary
            });

            // get value for Invoice Status in Array
            var invoiceStatus = result.getValue({
                name: result.columns[COLUMN_INDEX.INVOICE_STATUS].name,
                summary: result.columns[COLUMN_INDEX.INVOICE_STATUS].summary
            }).split(',');

            // get value for RevCom Status in Array
            var revrecStatus = result.getValue({
                name: result.columns[COLUMN_INDEX.REVCOM_STATUS].name,
                summary: result.columns[COLUMN_INDEX.REVCOM_STATUS].summary
            }).split(',');

            // push unique status to obj,revrec_status
            if(revrecStatus) obj.revcom_status = mergeAndGetUniqueStatus(obj.revcom_status, revrecStatus);

            // get unique invoice status in array
            if(invoiceStatus) obj.invoice_status = mergeAndGetUniqueStatus(obj.invoice_status, invoiceStatus);

            // get Sales Order Status
            obj.so_status = result.getText({
                name: result.columns[COLUMN_INDEX.SALES_ORDER_STATUS].name,
                summary: result.columns[COLUMN_INDEX.SALES_ORDER_STATUS].summary
            });

            if(balance > 0) {
                // balance is over 0
                if(invoiceStatus.length < 2 && invoiceStatus[0] == 'Paid In Full') {
                    // invoice is paid
                    obj.deferred += balance
                } else {
                    // invoice is not paid
                    obj.unearned += balance
                }
            } else if(balance < 0) {
                // negative balance is unbilled amount
                obj.unbilled += -balance
            }

            obj.balance += balance;

        });

        return obj;
    };

    exports.handleErrorIfAny = handleErrorIfAny;
    exports.calculateBalance = calculateBalance;
    exports.getCurrentDate = getCurrentDate;
    return exports;
});