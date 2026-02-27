// 日期格式化
function formatDate(date) {
    return date.toISOString().split('T')[0];
}

// 获取日期范围
function getDateRange(days = 30) {
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - days);
    return {
        start_time: formatDate(startDate),
        end_time: formatDate(endDate)
    };
}

// API数据获取 - 这个函数在base.html中已经定义，我们这里不再需要
// async function fetchAnalysisData(endpoint, params = {}) {
//     try {
//         const response = await fetch(`/api/${endpoint}`, {
//             method: 'POST',
//             headers: { 'Content-Type': 'application/json' },
//             body: JSON.stringify({ ...getDateRange(), ...params })
//         });
//         return response.ok ? await response.json() : null;
//     } catch (error) {
//         console.error('数据获取错误:', error);
//         return null;
//     }
// }

// 创建图表的通用函数 - 这个函数在base.html中已经定义，我们这里不再需要
// function createChart(canvasId, type, data, labels, title) {
//     const ctx = document.getElementById(canvasId)?.getContext('2d');
//     if (!ctx) return null;
//
//     return new Chart(ctx, {
//         type: type,
//         data: {
//             labels: labels,
//             datasets: [{
//                 label: title,
//                 data: data,
//                 backgroundColor: 'rgba(75, 192, 192, 0.2)',
//                 borderColor: 'rgb(75, 192, 192)',
//                 borderWidth: 1
//             }]
//         },
//         options: {
//             responsive: true,
//             maintainAspectRatio: false,
//             plugins: {
//                 title: {
//                     display: true,
//                     text: title
//                 }
//             }
//         }
//     });
// }

// 创建颜色化的条形图 - 此函数已经在页面特定的JS中实现，不再需要
// function createColoredBarChart(canvasId, data, labels, title, threshold1, threshold2) {
//     const ctx = document.getElementById(canvasId)?.getContext('2d');
//     if (!ctx) return null;
//     
//     // 为每个数据点生成颜色
//     const backgroundColors = data.map(value => {
//         if (value > threshold2) {
//             return 'rgba(255, 99, 132, 0.5)'; // 红色 - 严重
//         } else if (value > threshold1) {
//             return 'rgba(255, 205, 86, 0.5)'; // 黄色 - 中等
//         } else {
//             return 'rgba(75, 192, 192, 0.5)'; // 绿色 - 轻微
//         }
//     });
//     
//     const borderColors = data.map(value => {
//         if (value > threshold2) {
//             return 'rgb(255, 99, 132)'; // 红色 - 严重
//         } else if (value > threshold1) {
//             return 'rgb(255, 205, 86)'; // 黄色 - 中等
//         } else {
//             return 'rgb(75, 192, 192)'; // 绿色 - 轻微
//         }
//     });
//
//     return new Chart(ctx, {
//         type: 'bar',
//         data: {
//             labels: labels,
//             datasets: [{
//                 label: title,
//                 data: data,
//                 backgroundColor: backgroundColors,
//                 borderColor: borderColors,
//                 borderWidth: 1
//             }]
//         },
//         options: {
//             responsive: true,
//             maintainAspectRatio: false,
//             plugins: {
//                 title: {
//                     display: true,
//                     text: title
//                 }
//             }
//         }
//     });
// }

// 获取瓶颈等级HTML，带颜色样式
function getBottleneckLevelHTML(score) {
    if (score > 4.5) {
        return '<span class="text-danger">严重</span>';
    } else if (score > 2.5) {
        return '<span class="text-warning">中等</span>';
    } else {
        return '<span class="text-success">轻微</span>';
    }
}

// 获取瓶颈等级文本
function getBottleneckLevel(score) {
    if (score > 4.5) {
        return '严重';
    } else if (score > 2.5) {
        return '中等';
    } else {
        return '轻微';
    }
}

// 更新导航栏活动状态
document.addEventListener('DOMContentLoaded', () => {
    const currentPath = window.location.pathname;
    document.querySelector(`.nav-link[href="${currentPath}"]`)?.classList.add('active');
}); 