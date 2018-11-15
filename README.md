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

下一个正在注释的代码将是以及`t_list.c`，(我们打算跳过`hyperloglog.c`，你可以提交你的注释帮助我)

笔者阅读的顺序和更新列出的顺序是一样的，注释中可能有很多不是很好的地方，欢迎fork并提交你们的看法，我都会虚心的接受，或者联系本人的 QQ：918706091进行修改，多个人监督比一个人读效果更好。