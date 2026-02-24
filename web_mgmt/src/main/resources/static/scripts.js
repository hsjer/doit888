$(document).ready(function () {
    // 一级菜单展开/收起二级菜单
    $('.menu-item > span').click(function () {
        $(this).siblings('.submenu').slideToggle();
    });

    // 监听质量报告菜单点击
    $('#quality-report').click(function () {
        loadQualityReportPage();
    });

    // 监听新增规则菜单点击
    $('#add-rule').click(function () {
        loadAddRulePage();
    });

    // 监听“规则管理”菜单点击
    $('#rule-mgmt').click(function () {
        loadRuleMgmtPage();
    });


    // 监听“模型1”菜单点击
    $('#model1').click(function () {
        loadModel1Page();
    });


    // 监听“模型2”菜单点击
    $('#model2').click(function () {
        loadModel2Page();
    });
    // 加载质量报告内容
    function loadQualityReportPage() {
        const template = $('#quality-report-template').html();
        $('#page-content').html(template);
        initQualityReportHandlers();
    }

    // 加载新增规则内容
    function loadAddRulePage() {
        const template = $('#add-rule-template').html();
        $('#page-content').html(template);
        initAddRuleHandlers();
    }


    function loadRuleMgmtPage(){
        const template = $('#rule-template').html();
        $('#page-content').html(template);
        initRuleMgmtHandlers();
    }


    // 加载模型1页面
    function loadModel1Page() {
        const template = $('#model1-template').html();
        $('#page-content').html(template);
        initCrowdSelectionHandlers();
        initRuleParametersHandlers();
    }

    // 加载模型2页面
    function loadModel2Page() {
        const template = $('#model2-template').html();
        $('#page-content').html(template);
        //initCrowdSelectionHandlers();
        //initRuleParametersHandlers();
        initModel2Handlers();
    }

    // 规则管理页面事件绑定
    function initRuleMgmtHandlers(){
        $.ajax({
            url: '/api/mock/get_rules',  // 假设后端接口地址
            type: 'GET',
            data: {},
            dataType: 'json',
            success: function (response) {
                populateRuleTable('#rule-table', response.data);
            },
            error: function () {
                alert('获取数据失败，请稍后重试');
            }
        });
    }


    // 质量报告页面事件绑定
    function initQualityReportHandlers() {
        $('#view-report-btn').click(function () {
            const reportType = $('#report-select').val();
            const reportDate = $('#report-date').val();

            if (reportType && reportDate) {
                $.ajax({
                    url: '/api/get-log-report',  // 假设后端接口地址
                    type: 'GET',
                    data: {
                        type: reportType,
                        date: reportDate
                    },
                    dataType: 'json',
                    success: function (response) {
                        $('#table1-container').show();
                        populateTable('#table1', response.table1Data);

                        $('#table2-container').show();
                        populateTable('#table2', response.table2Data);

                        $('#table3-container').show();
                        populateTable('#table3', response.table3Data);

                        // 更新健康状态条
                        updateHealthStatusBar(response.healthy);
                    },
                    error: function () {
                        alert('获取数据失败，请稍后重试');
                    }
                });
            } else {
                alert('请选择报表和日期');
            }
        });
    }

    // 新增规则页面事件绑定
    function initAddRuleHandlers() {
        $('#submit-rule-btn').click(function () {
            const ruleType = $('#rule-select').val();
            const targetTable = $('#target-table').val();
            const targetField = $('#target-field').val();

            if (ruleType && targetTable && targetField) {
                $.ajax({
                    url: '/api/dq/check',  // 假设后端接口地址
                    type: 'POST',
                    contentType: 'application/json',
                    data: JSON.stringify({
                        ruleType: ruleType,
                        targetTable: targetTable,
                        targetField: targetField
                    }),
                    success: function (response) {
                        alert('规则提交成功！');
                        // 这里可以根据后端返回的响应进行处理
                    },
                    error: function () {
                        alert('提交失败，请稍后重试');
                    }
                });
            } else {
                alert('请填写完整信息');
            }
        });
    }

    // 填充表格的函数
    function populateRuleTable(tableSelector, data) {
        const tbody = $(tableSelector).find('tbody');
        tbody.empty();
        data.forEach(row => {
            const tr = $('<tr></tr>');
            const ruleId = row.id
            tr.append(`<td>${ruleId}</td>`);
            tr.append(`<td>${row.rule_name}</td>`);
            tr.append(`<td>${row.model_id}</td>`);
            tr.append(`<td>${row.crowd_cnt}</td>`);

            const json  = row.param_json;
            const subjson = json.substring(0,40)+"..."

            tr.append(`<td id="json" >${subjson}</td>`);
            const status = row.rule_status
            tr.append(`<td>${status}</td>`);
            tr.append(`<td>${row.hist_end}</td>`);
            tr.append(`<td>${row.creator}</td>`);
            tr.append(`<td>${row.auditor}</td>`);


            if(status === 1) {
                tr.append(`<td><button id="rule_on_${ruleId}" class="inline" disabled >上线</button><button class="inline" id="rule_off_${ruleId}">下线</button></td>`)
            }else{
                tr.append(`<td><button id="rule_on_${ruleId}" class="inline" >上线</button><button class="inline"  id="rule_off_${ruleId}" disabled>下线</button></td>`)
            }
            tbody.append(tr);
        });

        $('button[id^="rule_on_"]').on('click', function() {
            const id = $(this).attr("id").split("_").pop()


            $.get("/api/mock/changeStatus", { ruleId: id ,flag:1 })
                .done(function(data) {
                    console.log("Success:", data); // 请求成功时的回调

                    $(`#rule_on_${id}`).prop("disabled", true);
                    $(`#rule_off_${id}`).prop("disabled", false);
                })
                .fail(function(xhr, status, error) {
                    console.error("Error:", status, error); // 请求失败时的回调
                });



            // 这里可以添加其他逻辑
        });


        $('button[id^="rule_off_"]').on('click', function() {
            const id = $(this).attr("id").split("_").pop()

            $.get("/api/mock/changeStatus", { ruleId: id ,flag:0 })
                .done(function(data) {
                    console.log("Success:", data); // 请求成功时的回调

                    $(`#rule_on_${id}`).prop("disabled", false);
                    $(`#rule_off_${id}`).prop("disabled", true);
                })
                .fail(function(xhr, status, error) {
                    console.error("Error:", status, error); // 请求失败时的回调
                });



            // 这里可以添加其他逻辑
        });

    }


    // 填充表格的函数
    function populateTable(tableSelector, data) {
        const tbody = $(tableSelector).find('tbody');
        tbody.empty();
        data.forEach(row => {
            const tr = $('<tr></tr>');
            tr.append(`<td>${row.id}</td>`);
            tr.append(`<td>${row.rule_name}</td>`);
            tr.append(`<td>${row.model_id}</td>`);
            tr.append(`<td>${row.crowd_cnt}</td>`);
            tr.append(`<td>${row.param_json}</td>`);
            tr.append(`<td>${row.rule_status}</td>`);
            tr.append(`<td>${row.hist_end}</td>`);
            tr.append(`<td>${row.creator}</td>`);
            tr.append(`<td>${row.auditor}</td>`);
            tr.append('<td><button class="inline">上线</button><button class="inline">下线</button></td>')
            tbody.append(tr);
        });
    }

    // 更新健康状态条
    function updateHealthStatusBar(healthy) {
        const statusBar = $('#health-status-bar');
        if (healthy === 0) {
            statusBar.css({
                'background-color': 'red',
                'display': 'block'
            }).text('存在数据丢失');
        } else {
            statusBar.css({
                'background-color': 'green',
                'display': 'block'
            }).text('数据采集没问题');
        }
    }


    // 人群圈选表单处理
    function initCrowdSelectionHandlers() {
        let conditionCount = 0;
        let tagOptions = [];
        let tags = []
        $.ajax({
            url: '/api/get-tags',  // 假设后端接口地址
            type: 'GET',
            success: function (response) {
                tags = response.tags;
                /*tagOptions = response.tags.map(tag => `<option value="${tag.tag_name}">${tag.tag_alias}${tag.value_desc}</option>`).join('');*/

            },
            error: function () {
                alert('获取标签失败，请稍后重试');
            }
        });


        // 点击“添加条件”按钮
        $('#add-condition-btn').click(function () {
            let tag = tags[conditionCount];
            conditionCount++;
            const newConditionHtml = `
                        <div class="condition-row" data-index="${conditionCount}">
                            <select class="tag-select" data-condition-id="${conditionCount}">
                                /*${tagOptions}*/
                                <option value="${tag.tag_name}">${tag.tag_alias}</option>
                            </select>
                            
                            <select class="tag-operator">
                                <option value="eq">等于</option>
                                <option value="ne">不等于</option>
                                <option value="gt">大于</option>
                                <option value="gte"> >= </option>
                                <option value="lt">小于</option>
                                <option value="lte"> <= </option>
                                <option value="between">between</option>
                    
                            </select>
                            
                            <input type="text" class="tag-value-input" placeholder="${tag.value_desc}" />
                            <button type="button" class="remove-condition-btn">删除条件</button>
                        </div>
                    `;
            $('#condition-container').append(newConditionHtml);

        });

        // 删除条件按钮
        $(document).on('click', '.remove-condition-btn', function () {
            $(this).closest('.condition-row').remove();
        });

        // 点击“预圈选”按钮
        $('#submit-crowd-selection-btn').click(function () {
            const selectedConditions = [];
            $('#condition-container .condition-row').each(function () {
                const tag = $(this).find('.tag-select').val();
                const tag_operator = $(this).find('.tag-operator').val();
                const value = $(this).find('.tag-value-input').val();
                selectedConditions.push({ tag_name: tag,tag_operator:tag_operator, tag_value: value });
            });

            const requestData = {
                query_time: new Date().toISOString(),
                query_conditions: selectedConditions
            };

            $.ajax({
                url: '/api/mock/crowd-selection',  // 假设后端接口地址
                type: 'POST',
                contentType: 'application/json',
                data: JSON.stringify(requestData),
                success: function (response) {
                    $('#result').text('预选结果: ' + response.result);
                },
                error: function () {
                    alert('预圈选失败，请稍后重试');
                }
            });
        });
    }


    // 规则参数表单处理
    function initRuleParametersHandlers() {
        loadEventOptions();

        // 提交规则按钮点击事件
        $('#submit-rule-btn').click(function () {
            // 获取规则参数表单的数据
            const event = $('#event-select').val();
            const startTime = $('#start-time').val();
            const endTime = $('#end-time').val();

            // 获取人群圈选表单的数据
            const queryConditions = [];
            $('#condition-container .condition-row').each(function () {
                const tag = $(this).find('.tag-select').val();
                const tag_operator = $(this).find('.tag-operator').val();
                const value = $(this).find('.tag-value-input').val();
                queryConditions.push({ tag_name: tag,tag_operator:tag_operator, tag_value: value });
            });


            // 获取事件的props和对应的值
            const eventProps = [];
            $('#props-container .prop-row').each(function () {
                const propName = $(this).find('.prop-select').val();  // 获取prop的名称
                const propOperator = $(this).find('.prop-operator').val();  // 获取prop的运算符
                const propValue = $(this).find('.prop-value').val();  // 获取prop的值
                if (propName && propValue) {  // 如果propName和propValue都不为空，则添加到eventProps
                    eventProps.push({ prop_name: propName,prop_operator:propOperator ,prop_value: propValue });
                }
            });



            if (event && startTime && endTime) {
                const ruleData = {
                    event: event,
                    startTime: startTime,
                    endTime: endTime,
                    query_time: new Date().toISOString(),
                    crowdQueryTags: queryConditions, // 包含人群圈选条件
                    eventProps: eventProps  // 添加event_props
                };

                // 向后端发送合并后的数据
                $.ajax({
                    url: '/api/submit-rule',  // 后端接口地址
                    type: 'POST',
                    contentType: 'application/json',
                    data: JSON.stringify(ruleData),
                    success: function () {
                        alert('规则提交成功');
                    },
                    error: function () {
                        alert('规则提交失败，请稍后重试');
                    }
                });
            } else {
                alert('请填写完整的规则参数');
            }
        });


        // 获取事件选择框的选项
        function loadEventOptions() {
            $.ajax({
                url: '/api/get-events',  // 后端接口地址，假设返回事件选项
                type: 'GET',
                success: function (response) {
                    // 处理后端返回的事件数据
                    if (response && response.events && Array.isArray(response.events)) {
                        const eventOptions = response.events.map(event => {
                            // 确保props字段存在，且非空数组
                            const props = event.props && Array.isArray(event.props) ? event.props : [];
                            // 使用单引号包裹 JSON 字符串以避免 HTML 中的引号冲突
                            return `<option value="${event.id}" data-props='${JSON.stringify(props)}'>${event.name}</option>`;
                        }).join('');
                        $('#event-select').html(eventOptions);
                    } else {
                        $('#event-select').html('<option value="">没有可用的事件</option>');
                    }
                },
                error: function () {
                    $('#event-select').html('<option value="">加载失败，请稍后重试</option>');
                }
            });
        };

        // 当选择事件时，显示对应的props选择框
        $('#event-select').change(function () {
            const selectedEvent = $(this).find('option:selected');
            const eventId = selectedEvent.val();
            const props = JSON.parse(selectedEvent.attr('data-props'));

            // 清空之前的props选择框
            $('#props-container').empty();

            // 动态生成prop选择框和输入框
            props.forEach(function(prop) {
                const propRow = `
            <div class="prop-row">
                <select class="prop-select">
                    <option value="${prop}">${prop}</option>
                </select>
                <select class="prop-operator">
                    <option value="eq">等于</option>
                    <option value="ne">不等于</option>
                    <option value="gt">大于</option>
                    <option value="gte"> >= </option>
                    <option value="lt">小于</option>
                    <option value="lte"> <= </option>
                    <option value="between">between</option>
                    
                </select>
                <input type="text" class="prop-value" placeholder="请输入 ${prop} 的值">
                <button class="remove-prop">删除</button>
            </div>
        `;
                $('#props-container').append(propRow);
            });

            // 为删除按钮绑定点击事件
            $('.remove-prop').click(function() {
                $(this).closest('.prop-row').remove();
            });
            $('#props-container').show();
        });

        // 添加prop选项
        $('#add-prop-btn').click(function () {
            const propName = $('#prop-select').val();
            const propValue = $('#prop-value').val();

            if (propName && propValue) {
                selectedEventProps.push({ prop_name: propName, prop_value: propValue });

                // 在props输入框区域添加新选择项
                $('#props-container').append(`
                <div class="prop-row">
                    <span>${propName}:</span>
                    <input type="text" class="prop-value" value="${propValue}" />
                    <button type="button" class="remove-prop-btn">删除</button>
                </div>
            `);

                // 清空选择框
                $('#prop-select').val('');
                $('#prop-value').val('');
            } else {
                alert('请选择属性并填写值');
            }
        });

        // 删除prop选项
        $(document).on('click', '.remove-prop-btn', function () {
            const index = $(this).parent().index();
            selectedEventProps.splice(index, 1);
            $(this).parent().remove();
        });

    }



    function initModel2Handlers() {
        //loadEventOptions();

        let tag_idx = 0;
        let real_idx = 0;
        let real_prop_idx = 0;

        // 静态画像条件
        let tag_cons = []
        // 动态画像条件
        let real_cons = []
        // 动态画像事件属性
        let real_props = []

        // 规则完整参数
        const rule_param = {}

        // 获取事件选择框的选项
        function loadEventOptions() {
            $.ajax({
                url: '/api/mock/get-events',  // 后端接口地址，假设返回事件选项
                type: 'GET',
                success: function (response) {
                    // 处理后端返回的事件数据
                    if (response && response.events && Array.isArray(response.events)) {
                        const eventOptions = response.events.map(event => {
                            // 确保props字段存在，且非空数组
                            const props = event.props && Array.isArray(event.props) ? event.props : [];
                            // 使用单引号包裹 JSON 字符串以避免 HTML 中的引号冲突
                            return `<option value="${event.id}" data-props='${JSON.stringify(props)}'>${event.name}</option>`;
                        }).join('');
                        $('#event-select').html(eventOptions);
                    } else {
                        $('#event-select').html('<option value="">没有可用的事件</option>');
                    }
                },
                error: function () {
                    $('#event-select').html('<option value="">加载失败，请稍后重试</option>');
                }
            });
        };

        // 当选择事件时，显示对应的props选择框
        $('#event-select').change(function () {
            const selectedEvent = $(this).find('option:selected');
            const eventId = selectedEvent.val();
            const props = JSON.parse(selectedEvent.attr('data-props'));

            // 清空之前的props选择框
            $('#props-container').empty();

            // 动态生成prop选择框和输入框
            props.forEach(function(prop) {
                const propRow = `
            <div class="prop-row">
                <select class="prop-select">
                    <option value="${prop}">${prop}</option>
                </select>
                <select class="prop-operator">
                    <option value="eq">等于</option>
                    <option value="ne">不等于</option>
                    <option value="gt">大于</option>
                    <option value="gte"> >= </option>
                    <option value="lt">小于</option>
                    <option value="lte"> <= </option>
                    <option value="between">between</option>
                    
                </select>
                <input type="text" class="prop-value" placeholder="请输入 ${prop} 的值">
                <button class="remove-prop">删除</button>
            </div>
        `;
                $('#props-container').append(propRow);
            });

            // 为删除按钮绑定点击事件
            $('.remove-prop').click(function() {
                $(this).closest('.prop-row').remove();
            });
            $('#props-container').show();
        });

        // 添加标签条件按钮
        $('#tag-con-add').click(function(){
            const tagName = $('#tag-name-select').val();
            const tagOper = $('#tag-oper-select').val();
            const tagValue = $('#tag-value-input').val();


            $('#tag-s-container').append(`
                    <div id="tag-s-${tag_idx}">
                      <input class="inline"  disabled name="tag_name" value="${tagName}" />
                      <input class="inline"  disabled name="tag_oper" value="${tagOper}"/>
                      <input class="inline"  disabled name="tag_value" value="${tagValue}"/>
                      <button class="inline" name="tag-s-delete" type="button" id="${tag_idx}" >删除</button>
                    </div>
            `)

            tag_idx++;

            tag_cons.push({tag_name:tagName,tag_value:tagValue,tag_oper:tagOper})
        });


        // 删除标签条件按钮
        $(document).on('click', 'button[name="tag-s-delete"]', function () {
            $(this).parent().remove(); // 删除目标按钮所在的父 div
            const tmpId = $(this).attr('id');
            console.log('删除目标id: ' + tmpId)
            tag_cons[tmpId] = {}

        });


        //real-con-prop-con-add
        // 添加动态画像条件属性按钮
        $('#real-con-prop-add').click(function(){
            const realPropName = $('#real-con-prop-select').val();
            const realPropType = $('#real-con-prop-type-select').val();
            const realPropOper = $('#real-con-prop-oper-select').val();
            const realPropValue = $('#real-con-prop-value').val();


            $('#real-con-prop-container').append(`
                <div>
                  <input class="inline"  disabled name="real-con-prop-name" value="${realPropName}"/>
                  <input class="inline"  disabled name="real-con-prop-value-type" value="${realPropType}"/>
                  <input class="inline"  disabled name="real-con-prop-oper" value="${realPropOper}"/>
                  <input class="inline"  disabled name="real-con-prop-value" value="${realPropValue}"/>
                  <button class="inline" disabled name="real-con-prop-delete" id="${real_prop_idx}">删除</button>
                </div>
            `)

            real_prop_idx++;

            real_props.push({prop_name:realPropName,prop_type:realPropType,prop_oper:realPropOper,prop_value:realPropValue})
        });

        // 删除动态画像条件属性按钮
        $(document).on('click', 'button[name="real-con-prop-delete"]', function () {
            $(this).parent().remove(); // 删除目标按钮所在的父 div
            const tmpId = $(this).attr('id');

            real_props[tmpId] = {}
            console.log('删除目标属性id: ' + tmpId + ",删除后: " + JSON.stringify(real_props))
        });


        // 确认动态画像条件
        $('#real-con-add-btn').click(function(){
            const realEvent = $('#real-con-event-select').val();

            $('#real-con-container').append(`
               <div>
                   <button class="inline" name="real-con-delete" id="${real_idx}">删除</button>
                   <div class="inline" style="margin: 10px; padding: 5px; background-color: #a1e3bb; font-size: small">事件:${realEvent},属性约束:${JSON.stringify(real_props).replace(/"/g, '')}</div>
               </div>
            `)

            real_idx++;

            real_cons.push({event_id:realEvent,props:real_props})
            console.log(JSON.stringify(real_cons))
            console.log(JSON.stringify(real_props).replace(/"/g,''));

            $('#real-con-prop-container').html('')
            real_props=[]


        });

        // 删除动态画像条件
        $(document).on('click', 'button[name="real-con-delete"]', function () {
            $(this).parent().remove(); // 删除目标按钮所在的父 div
            const tmpId = $(this).attr('id');

            real_cons[tmpId] = {}
            console.log('删除目标id: ' + tmpId +",删除后: "+ JSON.stringify(real_cons))
        });



        // 提交规则  submit-model2-btn

        $('#submit-model2-btn').click(function(){

           const fireEvent = $('#fire-con-select').val()
           const fireOper = $('#fire-con-oper-select').val()
           const fireProp = $('#fire-con-prop-select').val()
           const fireValue = $('#fire-con-value').val()

           const fire_condition = {event_id:fireEvent,prop_name:fireProp,prop_oper:fireOper,prop_value:fireValue}


            const tagCons = tag_cons.filter(obj => Object.keys(obj).length > 0)
            const c1Seq = real_cons.filter(obj => Object.keys(obj).length > 0)


            rule_param['model_id'] = 3

            rule_param['static_profile_condition'] = tagCons

            const c1startTmp = $('#c1-start-time').val()
            const c1endTmp = $('#c1-end-time').val()


            const start_time = new Date(c1startTmp).getTime()

            let end_time = -1
            if(c1endTmp) {
                end_time = new Date(c1endTmp).getTime()
            }


            const seq_cnt_oper = $('#seq-cnt-oper-select').val()
            const seq_cnt_value = $('#seq-cnt-value').val()
            const cross_condition_id = 'c1'


            const cross_arr = []


            cross_arr.push({
                "start_time":start_time,
                "end_time":end_time,
                "seq_cnt_oper":seq_cnt_oper,
                "seq_cnt_value":seq_cnt_value,
                "cross_condition_id":cross_condition_id,
                "event_seq":c1Seq,
            })


            const c2_e = $('#c2-e-select').val()

            const startTmp = $('#c2-start-time').val();
            const c2start_time = new Date(startTmp).getTime()

            const endTmp = $('#c2-end-time').val();

            let c2end_time = -1
            if(endTmp){
                 c2end_time =  new Date(endTmp).getTime()
            }

            const c2prop = $('#c2-e-prop-select').val()
            const c2oper = $('#c2-e-prop-oper-select').val()
            const c2pvalue = $('#c2-e-prop-value').val()
            const c2cntoper = $('#c2-cnt-oper').val()
            const c2cntvalue = $('#c2-cnt_value').val()
            const c2id = 'c2'



            cross_arr.push({
                "start_time":c2start_time,
                "end_time":c2end_time,
                "event_id":c2_e,
                "event_cnt_oper":c2cntoper,
                "event_cnt_value":c2cntvalue,
                "prop_name":c2prop,
                "prop_oper":c2oper,
                "prop_avg_value":c2pvalue,
                "cross_condition_id":c2id
            })

            rule_param['cross_range_realtime_condition'] = cross_arr

            rule_param['fire_event_condition'] = fire_condition

            console.log(JSON.stringify(rule_param))

            // 向后端发送合并后的数据
            $.ajax({
                url: '/api/mock/submit-rule',  // 后端接口地址
                type: 'POST',
                contentType: 'application/json',
                data: JSON.stringify(rule_param),
                success: function () {
                    alert('规则提交成功');
                },
                error: function () {
                    alert('规则提交失败，请稍后重试');
                }
            });



        })


        $('#crowd-pre').click(function(){

            const requestData = {
                query_time: new Date().toISOString(),
                query_conditions: tag_cons
            };

            $.ajax({
                url: '/api/mock/crowd-selection',  // 假设后端接口地址
                type: 'POST',
                contentType: 'application/json',
                data: JSON.stringify(requestData),
                success: function (response) {
                    $('#crowd-pre-value').val('预估人数: ' + response.result);
                },
                error: function () {
                    alert('预圈选失败，请稍后重试');
                }
            });
        });

    }




});
