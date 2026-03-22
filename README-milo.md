# spring-ai-alibaba

## 项目信息
- 项目说明
  该项目是fork于开源SpringAiAlibaba。


- 源仓库
  [alibaba/spring-ai-alibaba](https://github.com/alibaba/spring-ai-alibaba)



## 多仓库配置说明
- 为了保证该项目后续能基于源仓库进行代码更新，可以考虑配置多个Git远程仓库。命令参考如下：

```
# 配置多仓库
$ git remote add source https://github.com/alibaba/spring-ai-alibaba.git

# 查看本地仓库
$ git remote -v
origin	git@gitlab.lizhi.fm:ocean/godzilla/web-ocean-comfyui-docker.git (fetch)
origin	git@gitlab.lizhi.fm:ocean/godzilla/web-ocean-comfyui-docker.git (push)
source	https://github.com/alibaba/spring-ai-alibaba.git (fetch)
source	https://github.com/alibaba/spring-ai-alibaba.git (push)

# 拉取源仓库的更新，注意处理冲突
$ git pull source main
```