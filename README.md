# redis-comment

对Redis5.0源码进行注释,该项目创建于2018年11月8日
现在已经完成的有

-  `sds.h`、
-  `sds.c`、
-  `adlist.h`、
-  `adlist.c`、
-  `dict.h`、
-  `dict.c` 
-  `server.h`(只对跳表的结构定义`zskiplistNode`以及`zskiplist`进行注释)
-  `t_zset.c`(只对其中以`zsl`开头的函数进行了注释，这是跳表的实现)
-  `intset.h`
-  `intset.c`
-  `ziplist.c`
-  `object.c`
-  `quicklist.h`
-  `quicklist.c`

回来继续更新，更新了`quicklist.h`以及`quicklist.c`的注释（2018年12月3日）

下一个正在注释的代码将是`t_list.c`，(我们打算跳过`hyperloglog.c`，你可以提交你的注释帮助我)

笔者阅读的顺序和更新列出的顺序是一样的，注释中可能有很多不是很好的地方，欢迎fork并提交你们的看法

2019年1月14日

又开始继续更新了，之前由于忙着其他事也就进度满了下来，现在考完试，乘着寒假好好的将重要的源码看一遍，这一次顺便会做出笔记，连同之前的数据结构和内存数据结构部分写一个比较完整的笔记，将首发于我的 GitHub 博客上面，敬请期待