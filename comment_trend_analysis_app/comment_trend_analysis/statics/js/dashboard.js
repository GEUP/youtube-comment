"use strict";

var datepicker = $('#datePicker');
var token_num = $('#token_num');

$('.date-search').click(function () {
    var searched_week_comment_cnt = $("#week_comment_cnt");
    var searched_day_comment_cnt = $("#day_comment_cnt");
    var week_up_down = $("#week_up_down");
    var day_up_down = $("#day_up_down");

    LoadingWithMask('statics/img/Spinner.gif');
    $.ajax({
        url: "/comment",
        data: { query_date: datepicker.val(), max_token_size: token_num.val() },
        method: "GET",
        dataType: "json"
    }).done(function (json) {

        $("#cloud_container").empty();
        var cloud_data = [];
        for (var t in json['sorted_token_cnt']) {
            var token = json['sorted_token_cnt'][t][0];
            cloud_data.push({
                "x": token,
                "value": json['sorted_token_cnt'][t][1],
                "similar_tokens": json['similar_token_dict'][token],
                "similar_scores": json['similar_token_score_dict'][token],
            });
        }
        var cloud_chart = anychart.tagCloud(cloud_data);
        cloud_chart.angles([0]);
        cloud_chart.container("cloud_container");
        cloud_chart.tooltip().format("similar_tokens: {%similar_tokens}\nsimilar_scores: {%similar_scores}")
        cloud_chart.draw();

        $("#tsne_container").empty();
        var tsne_data = [];
        var min_x = 0, max_x = 0, min_y = 0, max_y = 0;
        for (var t in json['tsne_list']) {
            var token = json['tsne_list'][t][0];
            var x = parseFloat(json['tsne_list'][t][1]);
            var y = json['tsne_list'][t][2];
            if (x < min_x) {
                min_x = x;
            }
            if (x > max_x) {
                max_x = x;
            }
            if (y < min_y) {
                min_y = y
            }
            if (y > max_y) {
                max_y = y
            }
            tsne_data.push({
                "token": token,
                "x": x,
                "value": y
            });
        }

        var x_scale = (max_x - min_x) / 30
        var y_scale = (max_y - min_y) / 30

        var tsne_chart = anychart.scatter();
        var controller = tsne_chart.annotations();
        var series = tsne_chart.marker(tsne_data);


        series.normal().size(2);
        series.hovered().size(3);

        var rect = controller.rectangle({
            xAnchor: min_x + x_scale,
            valueAnchor: min_y + y_scale,
            secondXAnchor: max_x - x_scale,
            secondValueAnchor: max_y - y_scale,
            fill: { opacity: 0 },
            hovered: {
                fill: { opacity: 0 }
            },
        });


        tsne_chart.xScale(anychart.scales.linear());
        tsne_chart.xScale().minimum(min_x - x_scale);
        tsne_chart.xScale().maximum(max_x + x_scale);
        tsne_chart.yScale().minimum(min_y - y_scale);
        tsne_chart.yScale().maximum(max_y + y_scale);
        tsne_chart.container("tsne_container");
        tsne_chart.labels().format("{%token}");
        tsne_chart.labels(true);
        tsne_chart.tooltip().titleFormat("{%token}");
        tsne_chart.tooltip().format("({%x}, {%value})");

        tsne_chart.label(1, { enabled: true, position: 'rightTop', anchor: 'rightTop', padding: 5, offsetX: 60, offsetY: 0, text: "View", background: { stroke: "1 black", enabled: true }, normal: { fontColor: "#000000" }, hovered: { fontColor: "#FF0000" } });
        tsne_chart.label(1).listen("click", function (e) {

            var x_scale = (rect.Oa.secondXAnchor - rect.Oa.xAnchor) / 30
            var y_scale = (rect.Oa.secondValueAnchor - rect.Oa.valueAnchor) / 30

            tsne_chart.xScale().minimum(rect.Oa.xAnchor);
            tsne_chart.yScale().minimum(rect.Oa.valueAnchor);
            tsne_chart.xScale().maximum(rect.Oa.secondXAnchor);
            tsne_chart.yScale().maximum(rect.Oa.secondValueAnchor);

            rect.xAnchor(rect.Oa.xAnchor + x_scale);
            rect.valueAnchor(rect.Oa.valueAnchor + y_scale);
            rect.secondXAnchor(rect.Oa.secondXAnchor - x_scale);
            rect.secondValueAnchor(rect.Oa.secondValueAnchor - y_scale);

            tsne_chart.draw();
        });

        tsne_chart.label(0, { enabled: true, position: 'rightTop', anchor: 'rightTop', padding: 5, offsetX: 5, offsetY: 0, text: "Home", background: { stroke: "1 black", enabled: true } });
        tsne_chart.label(0).listen("click", function (e) {

            tsne_chart.xScale().minimum(min_x - x_scale);
            tsne_chart.yScale().minimum(min_y - y_scale);
            tsne_chart.xScale().maximum(max_x + x_scale);
            tsne_chart.yScale().maximum(max_y + y_scale);

            rect.xAnchor(min_x + x_scale);
            rect.valueAnchor(min_y + y_scale);
            rect.secondXAnchor(max_x - x_scale);
            rect.secondValueAnchor(max_y - y_scale);
            tsne_chart.draw();

        });

        tsne_chart.draw();


        var week_up_down_percent = json['week_up_down_percent'].toFixed(2);
        var day_up_down_percent = json['day_up_down_percent'].toFixed(2);

        searched_week_comment_cnt.text(json['week_comment_cnt'].toString().replace(/\B(?<!\.\d*)(?=(\d{3})+(?!\d))/g, ","));
        searched_day_comment_cnt.text(json['day_comment_cnt'].toString().replace(/\B(?<!\.\d*)(?=(\d{3})+(?!\d))/g, ","));

        function up_down_change(up_down, up_down_percent) {
            var up_down_icon = up_down.children()
            up_down.removeClass("text-danger");
            up_down.removeClass("text-success");
            up_down_icon.removeClass("fa-arrow-up");
            up_down_icon.removeClass("fa-arrow-down");
            if (up_down_percent >= 0) {
                up_down.addClass("text-success");
                up_down.contents()[1].textContent = ' ' + up_down_percent + '%';
                up_down_icon.addClass("fa-arrow-up");
            } else {
                up_down.addClass("text-danger");
                up_down.contents()[1].textContent = ' ' + up_down_percent + '%';
                up_down_icon.addClass("fa-arrow-down");
            }
        };

        up_down_change(week_up_down, week_up_down_percent);
        up_down_change(day_up_down, day_up_down_percent);
        closeLoadingWithMask();
    })

})

function LoadingWithMask(gif) {
    //화면의 높이와 너비를 구합니다.
    var maskHeight = $(document).height();
    var maskWidth = window.document.body.clientWidth;

    //화면에 출력할 마스크를 설정해줍니다.
    var mask = "<div id='mask'><img id='loadingImg' src='" + gif + "'/></div>";


    //화면에 레이어 추가
    $('body').append(mask)

    //마스크의 높이와 너비를 화면 것으로 만들어 전체 화면을 채웁니다.
    $('#mask').css({
        'width': maskWidth,
        'height': maskHeight,
        'opacity': '0.3'
    });

    //마스크 표시
    $('#mask').show();

    //로딩중 이미지 표시
    //$('#loadingImg').append(loadingImg);
    //$('#loadingImg').show();
}

function closeLoadingWithMask() {
    $('#mask').hide();
}

