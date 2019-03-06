var init = true;
var portNo = 8080;

$(document).ready(function() {
	getData();
})

function hideAllTable() {
	$("#applicationTable_wrapper").hide();
}

function getData() {
	var selectApp = $('[name=applications]').val();
	$.ajax({
		type : 'GET',
		url : "http://" + location.hostname + ":" + portNo + "/api?dn=hist&ap=" + selectApp
	}).done(function(data) {
		createChart(data, selectApp);
		createTable(data);
	}).fail(function(data) {
		handlingOfFailedToLoad();
	});
}

function handlingOfFailedToLoad() {
	$("#msgFailToGetData").show();
}

function createChart(data, selectApp) {
	if(data.length == 0){
   		return;
 	}
	var startedTimes = [];
	var elapsedTimes = [];
	for (i = 0; i < data.length; i++) {
		startedTimes.push(data[i]["startedTime"]);
		elapsedTimes.push(convert2Sec(data[i]["elapsedTime"]));
		console.log(convert2Sec(data[i]["elapsedTime"]));
	}
	
	var ctx = document.getElementById('myChart').getContext('2d');
    var myChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: startedTimes,
            datasets: [{
                label: selectApp,
                type: "line",
                fill: false,
                lineTension: 0,
                data: elapsedTimes,
                borderColor: "rgb(154, 162, 235)",
                yAxisID: "y-axis-1",
            }]
        },
        options: {
            tooltips: {
                mode: 'nearest',
                intersect: false,
            },
            responsive: true,
            scales: {
                yAxes: [{
                    id: "y-axis-1",
                    type: "linear",
                    position: "left",
                    ticks: {
                        max: 100,
                        min: 0,
                        stepSize: 10
                    },
                }, {
                    id: "y-axis-2",
                    type: "linear",
                    position: "right",
                    ticks: {
                        max: 200,
                        min: 0,
                        stepSize: 5
                    },
                    gridLines: {
                        drawOnChartArea: false,
                    },
                }],
            },
        }
    });
}

function convert2Sec(t) {
	var hms = t.split(':');
	return Number(hms[0]) * 3600 + Number(hms[1]) * 60 + Number(hms[2]);
}

function createTable(data) {
	if(data.length == 0){
   		hideAllTable();
   		$("#msgNoData").show();
   		return;
 	} 
 	$("#msgNoData").hide();
 	$("#msgFailToGetData").hide();

	resultList = [];
	var columns = Object.keys(data[0]);
	for (i = 0; i < data.length; i++) {
		var miniList = [];
		for (j = 0; j < columns.length; j++) {
			miniList.push(data[i][columns[j]]);
		}
		resultList.push(miniList);
	}

	// Create result for table
	var resultConfig = {
		"bDestroy" : true,
		"bFilter" : false,
		"bStateSave" : true,
		"aaData" : resultList,
		"aaSorting" : [[0, "desc"]],
		"aoColumns" : [],
		"dom" : "Bfrtip",
		"buttons" : [ "csv" ]
	};

	// Add column info
	for (j = 0; j < columns.length; j++) {
		resultConfig.aoColumns.push({
			"sTitle" : columns[j],
			"sClass" : "center"
		});
	}
	
	// Show table
	$('#applicationTable').DataTable(resultConfig);
	$("#applicationTable_wrapper").show();
	
	var currentTime = new Date();
	var month = toDoubleDigits(currentTime.getMonth() + 1);
	var day = toDoubleDigits(currentTime.getDate());
	var hour = toDoubleDigits(currentTime.getHours());
	var minute = toDoubleDigits(currentTime.getMinutes());
	var second = toDoubleDigits(currentTime.getSeconds());
	var date = "Last Update" + "(" + month + "/" + day + " " + hour + ":" + minute + ":" + second + ")";
	
	$("#last-update").text(date);
	if (init) {
		setInterval(autoUpdate, 10000);
	}	
}

function autoUpdate() {
	init = false;
	if(document.getElementById('autoupdate').checked){
		getData();
	}
}

function toDoubleDigits(num) {
	num += "";
	if (num.length === 1) {
		num = "0" + num;
	}
	return num;     
}
