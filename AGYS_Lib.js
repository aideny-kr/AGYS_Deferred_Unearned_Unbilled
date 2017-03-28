/**
 * @NApiVersion 2.x
 * @NModuleScope Public
 */

define(['N/record', 'N/error', 'N/email', 'N/runtime', 'N/format', 'N/search'],
    function(record, error, email, runtime, format, search){

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
    };

    var getCurrentDateTime = function() {
        var parsedDateString = format.parse({
            value: new Date(),
            type: format.Type.DATE,
            timezone: format.Timezone.AMERICA_NEW_YORK
        });

        return format.format({
            value: parsedDateString,
            type: format.Type.DATETIME
        });
    };

    /**
     * parses date parameter and returns date of one last day of the parameter
     * @param date {String}
     * @returns {String}
     */

    var getOneDayLessDate = function(date) {
        log.audit({
            title: 'getOneDayLessDate date param',
            details: date
        });

        var parsedRawDateObject = format.parse({
            value: date,
            type: format.Type.DATE
        });

        var year = parsedRawDateObject.getFullYear();
        var dates = parsedRawDateObject.getDate() - 1;
        var month = parsedRawDateObject.getMonth() + 1;

        var newParsedRawDateObject =  format.parse({
            value: new Date(month + '/' + dates + '/' + year),
            type: format.Type.DATE
        });

        return format.format({
            value: newParsedRawDateObject,
            type: format.Type.DATE
        });

    };


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
     * calculates deferred/unearned/unbilled and balance and return the calculated result in object
     * @param {object} from NetSuite search
     * @param {object} initialzed object
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


    /**
     * Calculate total amounts and create a Summary Record
     * @param summary NetSuite Summary Object
     */

    var writeSummaryRecord = function(summary) {
        var data = summary.output;
        var asOfDate = getCurrentDate();
        var deferred = unearned = unbilled = balance = 0;

        log.debug({
            title: 'writeSummaryRecord data value',
            details: data
        });

        // iterating each object in summary
        data.iterator().each(function(key, value) {

            log.debug({
                title: 'In summary iterator, id: ' + key,
                details: typeof value + ': ' + value
            });


            value = JSON.parse(value);

            deferred += +value.deferred;
            unearned += +value.unearned;
            unbilled += +value.unbilled;
            balance += +value.balance;

            // don't forget
            return true;
        });

        log.audit({
            title: 'Values in Summary',
            details: 'deferred: ' + deferred + ', unearned: ' + unearned + ', unbilled: ' + unbilled + ', balance: '
            + balance
        });

        upsertNewSummaryRecord(asOfDate, deferred, unearned, unbilled, balance);

    };

    var upsertNewSummaryRecord = function(asOfDate, deferred, unearned, unbilled, balance) {
        var dateLessOneDay = getOneDayLessDate(asOfDate);
        var currentDate = getCurrentDate();
        var currentDateTime = getCurrentDateTime();
        var existing_record_id = 0;

        log.debug({
            title: 'dateLessOneDay value',
            details: dateLessOneDay
        });

        // find if there is a record that was generated between today and yesterday.
        search.create({
            type: 'customrecord_ag_revenue_classfication',
            filters: [
                ['custrecord_ag_as_of_date', 'between', asOfDate, dateLessOneDay]
            ]
        }).run().each( function(each){
            if(each.hasOwnProperty('id')) {
                existing_record_id = +each.id;
            }
            return false;
        });

        if(existing_record_id != 0) {
            // record already exists, write the values to the record.
            var existingRec = record.load({
                type: 'customrecord_ag_revenue_classfication',
                id: existing_record_id
            });

            existingRec.setValue({
                fieldId: 'custrecord_ag_deferred_total',
                value: deferred
            });

            existingRec.setValue({
                fieldId: 'custrecord_ag_total_unearned',
                value: unearned
            });

            existingRec.setValue({
                fieldId: 'custrecord_ag_total_unbilled',
                value: unbilled
            });

            existingRec.setValue({
                fieldId: 'custrecord_ag_total_balance',
                value: balance
            });

            existingRec.setValue({
                fieldId: 'custrecord_ag_as_of_date',
                value: currentDateTime
            });

            try {
                existingRec.save();

            } catch(err) {
                log.error({
                    title: 'Error saving existing custom record',
                    details: err
                });
            }

        } else {
            // create a new record.
            var newRec = record.create({
                type: 'customrecord_ag_revenue_classfication'
            });

            newRec.setValue({
                fieldId: 'custrecord_ag_deferred_total',
                value: deferred
            });

            newRec.setValue({
                fieldId: 'custrecord_ag_total_unearned',
                value: unearned
            });

            newRec.setValue({
                fieldId: 'custrecord_ag_total_unbilled',
                value: unbilled
            });

            newRec.setValue({
                fieldId: 'custrecord_ag_total_balance',
                value: balance
            });

            newRec.setValue({
                fieldId: 'custrecord_ag_as_of_date',
                value: currentDateTime
            });

            try {
                newRec.save();

            } catch(err) {
                log.error({
                    title: 'Error saving new custom record',
                    details: err
                });
            }

        }
    };

    exports.handleErrorIfAny = handleErrorIfAny;
    exports.calculateBalance = calculateBalance;
    exports.getCurrentDate = getCurrentDate;
    exports.writeSummaryRecord = writeSummaryRecord;

    return exports;
});