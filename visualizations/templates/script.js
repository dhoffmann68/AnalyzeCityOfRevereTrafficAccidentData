// Our labels along the x-axis

var years = labels// For drawing the lines

var cluster1= values[0];

var cluster2= values[1];

var cluster3= values[2];


var ctx = document.getElementById("myChart");

var myChart = new Chart(ctx, {
  
type: 'line',
  
data: {
labels: years,
    
datasets: [
      
{ 
 data: cluster1, label: "Cluster 1",
  borderColor: "#3e95cd",
fill: false
      },
      
{ 
 data: cluster2, label: "Cluster 2",
borderColor: "#8e5ea2",
 fill: false
      },
      
{ 
 data: cluster3, label: "Cluster 3",
  borderColor: "#3cba9f",
 fill: false
      }
 ]
  }
});