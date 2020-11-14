<%@page import="java.util.ArrayList"%>
<%@ page language="java" contentType="text/html; charset=utf-8"
         pageEncoding="utf-8"%>
<!DOCTYPE html>
<html>
<head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="stylesheet" href="https://cdn.staticfile.org/twitter-bootstrap/4.3.1/css/bootstrap.min.css">
    <script src="https://cdn.staticfile.org/jquery/3.2.1/jquery.min.js"></script>
    <script src="https://cdn.staticfile.org/popper.js/1.15.0/umd/popper.min.js"></script>
    <script src="https://cdn.staticfile.org/twitter-bootstrap/4.3.1/js/bootstrap.min.js"></script>
    <script src="${pageContext.request.contextPath }/js/echarts.min.js"></script>
    <title>Insert title here</title>
    <style>
        .form-group{
            width:35%;
            float:left;
            margin-left:10px;
            margin-top:10px;
        }
        label{
            width:30%;
            float:left;
            margin-right:5px;
            margin-top:5px;
        }
        .form-control{
            width:60%;
        }
        .btn{
            width:100px;
            height:40px;
            border:0px;
            border-radius:5px;
            background-color:orange;
            color:black;
            margin-left:20px;
            margin-top:10px
        }
    </style>
</head>
<body>
<div id="main" style="width: 1000px;height: 600px;"></div>
<table class="table table-bordered table-hover" id="table">
    <thead>
    <tr>
        <th>动漫名称</th>
        <th>喜欢人数</th>
    </tr>
    </thead>
    <tbody>
    <tr id="tr1">
        <td>火影忍者</td>
        <td>500</td>
    </tr>
    <tr id="tr2">
        <td>海贼王</td>
        <td>300</td>
    </tr>
    <tr id="tr3">
        <td>妖精的尾巴</td>
        <td>200</td>
    </tr>
    <tr id="tr4">
        <td>死神</td>
        <td>400</td>
    </tr>
    <tr id="tr5">
        <td>七龙珠</td>
        <td>600</td>
    </tr>
    </tbody>
</table>
<script type="text/javascript">
    // 页面加载函数
    //进行echarts的初始化
    var myEcharts = echarts.init(document.getElementById("main"));
    var option = {
        // 定义标题
        title : {
            text:"环形图示例"
        },
        // 鼠标悬停显示数据
        tooltip:{
        },
        //图例
        legend : {
            data: ['火影忍者','海贼王','妖精的尾巴','死神','七龙珠']
        },
        //数据
        series :[
            {
                radius:['55%','70%'], //半径
                label:{
                    normal:{
                        // 取消在原来的位置显示
                        show:false,
                        // 在中间显示
                        position:'center'
                    },
                    // 高亮扇区
                    emphasis:{
                        show:true,
                        textStyle:{
                            fontSize:30,
                            fontWeight:'bold'
                        }
                    }
                },
                data:[
                    // 对应图例的值
                    {name:'火影忍者',value:500},
                    {name:'海贼王',value:300},
                    {name:'妖精的尾巴',value:200},
                    {name:'死神',value:400},
                    {name:'七龙珠',value:600}
                ],
                type:'pie',
                //关掉南丁格尔图
                //roseType:'radius'
            }
        ]
    };
    // 设置配置项
    myEcharts.setOption(option);
    // 设置echarts的点击事件
    myEcharts.on('click',function (params) {
        // 获取table下所有的tr
        let trs = $("#table tbody tr");
        for (let i = 0;i<trs.length;i++){
            // 获取tr下所有的td
            let tds = trs.eq(i).find("td");
            // 先把之前的标记的success去掉
            $("#table tbody tr").eq(i).removeClass('success');
            // 如果点击图示的名字和table下的某一个行的第一个td的值一样
            if (params.name == tds.eq(0).text()){
                //设置success状态
                $("#table tbody tr").eq(i).addClass('success');
                // 跳转到页面指定的id位置
                $("html,body").animate({scrollTop:$("#table tbody tr").eq(i).offset().top},1000);
            }
        }
    });
    // 当鼠标落在tr时，显示浮动
    $("#table tbody").find("tr").on("mouseenter",function () {
        // 获得当前匹配元素的个数
        let row = $(this).prevAll().length;
        // 获得当前tr下td的名字
        let name = $("#table tbody").find("tr").eq(row).find("td").eq(0).text();
        // 设置浮动
        myEcharts.dispatchAction({ type: 'showTip',seriesIndex: 0, name:name});//选中高亮
    });
    // 当鼠标移开tr时候取消浮动
    $("#table tbody").find("tr").on("mouseleave",function () {
        // 获得当前匹配元素的个数
        let row = $(this).prevAll().length;
        // 获得当前tr下td的名字
        let name = $("#table tbody").find("tr").eq(row).find("td").eq(0).text();
        // 设置浮动
        myEcharts.dispatchAction({ type: 'hideTip', name:name});//选中高亮
    });
</script>
</body>
</html>