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
$ git remote set-url source git@github.com:alibaba/spring-ai-alibaba.git

# 查看本地仓库
$ git remote -v
origin  git@github.com:wzmmao/spring-ai-alibaba.git (fetch)
origin  git@github.com:wzmmao/spring-ai-alibaba.git (push)
source  git@github.com:alibaba/spring-ai-alibaba.git (fetch)
source  git@github.com:alibaba/spring-ai-alibaba.git (push)



# 对齐源仓库的tag列表
$ git fetch source --tags
```

- 同步源仓库的main分支的代码更新
```
# 本地拉取源仓库的更新，注意处理冲突
$ git pull source main

# 本地拉取源仓库更新后推送到私人fork仓库
$ git push origin main
```

- 同步源仓库的tags更新
```
# 本地拉取源仓库的tag更新
$ git fetch source --tags

# 本地拉取源仓库tag更新后推送到私人fork仓库
$ git push origin --tags
```
