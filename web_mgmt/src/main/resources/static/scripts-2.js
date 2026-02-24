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


    // 监听“模型1”菜单点击
    $('#model1').click(function () {
        loadModel1Page();
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


    // 加载模型1页面
    function loadModel1Page() {
        const template = $('#model1-template').html();
        $('#page-content').html(template);
        initCrowdSelectionHandlers();
        initRuleParametersHandlers();
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
    function populateTable(tableSelector, data) {
        const tbody = $(tableSelector).find('tbody');
        tbody.empty();
        data.forEach(row => {
            const tr = $('<tr></tr>');
            row.forEach(cell => {
                tr.append(`<td>${cell}</td>`);
            });
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

        // 点击“添加条件”按钮
        $('#add-condition-btn').click(function () {
            conditionCount++;
            $.ajax({
                url: '/api/get-tags',  // 假设后端接口地址
                type: 'GET',
                success: function (response) {
                    const tagOptions = response.tags.map(tag => `<option value="${tag.tag_name}">${tag.tag_alias}${tag.value_desc}</option>`).join('');
                    const newConditionHtml = `
                        <div class="condition-row" data-index="${conditionCount}">
                            <select class="tag-select" data-condition-id="${conditionCount}">
                                ${tagOptions}
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
                            
                            <input type="text" class="tag-value-input" placeholder="输入条件值" />
                            <button type="button" class="remove-condition-btn">删除条件</button>
                        </div>
                    `;
                    $('#condition-container').append(newConditionHtml);
                },
                error: function () {
                    alert('获取标签失败，请稍后重试');
                }
            });
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
                url: '/api/crowd-selection',  // 假设后端接口地址
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







});
