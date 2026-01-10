local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")
local utf8 = require("utf8")
local html_entities = require("htmlEntities")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_user = nil

local workspace_type = cjson.decode(os.getenv("workspace_type"))

local url_count = 0
local tries = 0
local downloaded = {}
local seen_200 = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local discovered_stash_items = {}
local bad_items = {}
local ids = {}

local retry_url = false
local is_initial_url = true

local item_patterns = {
  ["^https?://api%.bitbucket%.org/2%.0/repositories%?after=([0-9][0-9][0-9][0-9]%-[0-9][0-9]%-[0-9][0-9]T[0-9][0-9]%%3A[03])"] = "repo-disco",
  ["^https?://api%.bitbucket%.org/2%.0/workspaces/([%-%._0-9a-zA-Z%%]+)$"] = "workspace",
  ["^https?://api%.bitbucket%.org/2%.0/repositories/([^/]+/[^/%?&]+)$"] = "repo"
}

local disco_patterns = {
  ["^https?://api%.bitbucket%.org/2%.0/repositories/([%-%._0-9a-zA-Z%%]+/[%-%._0-9a-zA-Z%%]+)"] = "repo",
  ["^https?://bitbucket%.org/([%-%._0-9a-zA-Z%%]+/[%-%._0-9a-zA-Z%%]+)"] = "repo"
}

abort_item = function(item)
  abortgrab = true
  --killgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
--print("discovered", target, item)
    target[item] = true
    return true
  end
  return false
end

find_item = function(url)
  if ids[url] then
    return nil
  end
  url = string.gsub(url, "%%7[BD]", "|")
  local value = nil
  local type_ = nil
  for pattern, name in pairs(item_patterns) do
    value = string.match(url, pattern)
    type_ = name
    if value then
      break
    end
  end
  if value and type_ then
    if type_ == "workspace" then
      type_ = workspace_type[value]
      assert(type_)
    end
    return {
      ["value"]=value,
      ["type"]=type_
    }
  end
end

finalize_item = function()
  if item_type == "workspace-check"
    and not context["recent_update"] then
    print("Workspace did not have a recent update, queueing.")
    discover_item(discovered_items, "workspace:" .. item_value)
  end
end

set_item = function(url)
  found = find_item(url)
  if found then
    local newcontext = {}
    new_item_type = found["type"]
    new_item_value = found["value"]
    new_item_name = new_item_type .. ":" .. new_item_value
    if new_item_type == "repo-disco" then
      local first_part, last_num = string.match(new_item_value, "^(.+)([0-9])$")
      newcontext["first_part"] = first_part
      newcontext["last_num"] = tonumber(last_num)
    elseif new_item_type == "repo" then
      local user, name = string.match(new_item_value, "^([^/]+)/([^/]+)$")
      newcontext["user"] = user
      newcontext["name"] = name
    end
    if new_item_name ~= item_name then
      if item_name then
        finalize_item()
      end
      ids = {}
      context = newcontext
      item_value = new_item_value
      item_type = new_item_type
      ids[string.lower(item_value)] = true
      abortgrab = false
      tries = 0
      retry_url = false
      is_initial_url = true
      item_name = new_item_name
      print("Archiving item " .. item_name)
    end
  end
end

percent_encode_url = function(url)
  temp = ""
  for c in string.gmatch(url, "(.)") do
    local b = string.byte(c)
    if b < 32 or b > 126 then
      c = string.format("%%%02X", b)
    end
    temp = temp .. c
  end
  return temp
end

allowed = function(url, parenturl)
  local noscheme = string.match(url, "^https?://(.*)$")

  if ids[url]
    or (noscheme and ids[string.lower(noscheme)]) then
    return true
  end

  if string.match(url, "%.git$")
    or string.match(url, "%?q=project%.key=$")
    or string.match(url, "^https?://bitbucket%.org/[^/]+/[^/]+/commits/")
    or string.match(url, "^https?://bitbucket%.org/[^/]+/[^/]+/packages/")
    or string.match(url, "^https?://api%.bitbucket%.org/[^/]+/[^/]+/src/[0-9a-f]+/&$")
    or string.match(url, "^https?://bitbucket%.org/[^/]+/[^/]+/pull%-requests/new")
    or string.match(url, "^https?://bitbucket%.org/[^/]+/[^/]+/get/[0-9a-f]+%.tar%.bz2$")
    or string.match(url, "^https?://bitbucket%.org/[^/]+/[^/]+/get/[0-9a-f]+%.tar%.gz$") then
    return false
  end

  for _, api in pairs({
    "^https?://api%.bitbucket%.org/2%.0/",
    "^https?://bitbucket%.org/!api/2%.0/",
    "^https?://bitbucket%.org/!api/internal/"
  }) do
    if string.match(url, api .. ".-/hooks$")
      or string.match(url, api .. "repositories/[^/]+/[^/]+/downloads$")
      or string.match(url, api .. "repositories/[^/]+/[^/]+/src/")
      or string.match(url, api .. "repositories/[^/]+/[^/]+/commit/")
      or string.match(url, api .. "repositories/[^/]+/[^/]+/filehistory/")
      or string.match(url, api .. "repositories/[^/]+/[^/]+/patch/")
      or string.match(url, api .. "repositories/[^/]+/[^/]+/diff/")
      or string.match(url, api .. "repositories/[^/]+/[^/]+/commits/[^%?]+%?.-ctx=")
      or string.match(url, api .. "repositories/[^/]+/[^/]+/commits%?.-ctx=")
      or string.match(url, api .. "repositories/[^/]+/[^/]+/changeset/[0-9a-f]+$")
      or string.match(url, api .. "repositories/[^/]+/[^/]+/pullrequests/[0-9]+/decline")
      or string.match(url, api .. "repositories/[^/]+/[^/]+/pullrequests/[0-9]+/approve")
      or string.match(url, api .. "repositories/[^/]+/[^/]+/pullrequests/[0-9]+/merge")
      or string.match(url, api .. "repositories/[^/]+/[^/]+/pullrequests/[0-9]+/statuses")
      or string.match(url, api .. "repositories/[^/]+/[^/]+/pullrequests/[0-9]+/request%-changes") then
      return false
    end
  end

  url = string.gsub(url, "%%7B", "{")
  url = string.gsub(url, "%%7D", "}")

  local skip = false
  for _, patterns in pairs({item_patterns, disco_patterns}) do
    for pattern, type_ in pairs(patterns) do
      match = string.match(url, pattern)
      if match and not string.match(match, "%.git$") then
        local new_item = type_ .. ":" .. match
        if new_item ~= item_name then
          if item_type ~= "repo-disco"
            and not string.match(type_, "^workspace") then
            discover_item(discovered_items, new_item)
            skip = true
          end
        end
      end
    end
  end
  if skip then
    return false
  end

  if not string.match(url, "^https?://[^/]*bitbucket%.com/")
    and not string.match(url, "^https?://[^/]*bitbucket%.org/") then
    discover_item(discovered_outlinks, string.match(percent_encode_url(url), "^([^%s]+)"))
    return false
  end

  if item_type == "repo-disco" then
    local a, b = string.match(url, "^https?://api%.bitbucket%.org/2%.0/repositories%?after=([0-9][0-9][0-9][0-9]%-[0-9][0-9]%-[0-9][0-9]T[0-9][0-9]%%3A)([0-9])")
    if a and b then
      b = tonumber(b)
      if a == context["first_part"]
        and b >= context["last_num"]
        and b < context["last_num"] + 3 then
        return true
      end
    end
  end

  for _, pattern in pairs({
    "([%-%._0-9a-zA-Z%%]+)",
    "([%-%._0-9a-zA-Z%%]+/[%-%._0-9a-zA-Z%%]+)",
  }) do
    for s in string.gmatch(url, pattern) do
      if ids[string.lower(s)] then
        return true
      end
    end
  end

  if item_type == "repo" then
    for _, pattern in pairs({
      "([^/]+)",
      "([%-%._0-9a-zA-Z%%]+)"}) do
      local prev = nil
      for s in string.gmatch(url, pattern) do
        if prev and (
          prev .. "/" .. s == item_value
          or prev .. "/" .. tostring(string.match(s, "^(.-)%.[^%.]+$")) == item_value
        ) then
          return true
        end
        prev = s
      end
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  --[[if allowed(url, parent["url"])
    and not processed(url)
    and string.match(url, "^https://")
    and not addedtolist[url] then
    addedtolist[url] = true
    return true
  end]]

  return false
end

decode_codepoint = function(newurl)
  newurl = string.gsub(
    newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
    function (s)
      return utf8.char(tonumber(s, 16))
    end
  )
  return newurl
end

percent_encode_url = function(newurl)
  result = string.gsub(
    newurl, "(.)",
    function (s)
      local b = string.byte(s)
      if b < 32 or b > 126 then
        return string.format("%%%02X", b)
      end
      return s
    end
  )
  return result
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  local json = nil

  local post_data = nil

  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function fix_case(newurl)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl)
    if not string.match(newurl, "^https?://") then
      return nil
    end
    local post_body = nil
    local post_url = nil
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0
      or string.len(newurl) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_ .. tostring(post_data))
      and allowed(url_, origurl) then
      local headers = {}
      if string.match(url_, "%.git/info/refs%?service=git%-upload%-pack$") then
        headers["Accept"] = "*/*"
      elseif string.match(url_, "%.git/git%-upload%-pack$") then
        headers["Accept"] = "application/x-git-upload-pack-result"
        headers["Content-Type"] = "application/x-git-upload-pack-request"
      end
      if string.match(url_, "%.git/") then
        headers["User-Agent"] = "git/2.30.2"
        headers["Pragma"] = "no-cache"
        headers["Git-Protocol"] = "version=2"
      end
      if string.match(url, "^https?://[^/]*bitbucket%.org/!api/") then
        headers["Accept"] = "application/json"
        headers["Content-Type"] = "application/json"
        headers["X-Bitbucket-Frontend"] = "frontbucket"
        --headers["X-CSRFToken"] = "ublzwclA6gzbpBKjW542uLBDkpDV5jUr"
        headers["X-Requested-With"] = "XMLHttpRequest"
      end
      if post_data and string.match(url_, "/commits/statuses/%?") then
        headers["Accept"] = "*/*"
        if string.match(post_data, "&") then
          headers["Content-Type"] = "text/plain;charset=UTF-8"
        else
          headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
        end
      end
      if string.match(url, "[%?&]_pjax=") then
        headers["X-PJAX"] = "true"
        headers["X-PJAX-Container"] = "#downloads .downloads-pjax-container"
        headers["X-Requested-With"] = "XMLHttpRequest"
      end
      if item_type == "workspace" and post_data then
        headers["Content-Type"] = "application/json"
        --headers["X-API-KEY"] = "e68da679-ff2b-4bae-913d-22d58892baa8"
        headers["X-Client-Name"] = "feature-gate-js-client"
        headers["X-Client-Version"] = "5.3.0"
      end
--print('queueing', url_, cjson.encode(headers), post_data)
      if post_data then
        table.insert(urls, {
          url=url_,
          headers=headers,
          body_data=post_data,
          method="POST"
        })
      else
        table.insert(urls, {
          url=url_,
          headers=headers
        })
      end
      addedtolist[url_ .. tostring(post_data)] = true
    end
  end

  local function checknewurl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function set_new_params(newurl, data)
    for param, value in pairs(data) do
      if value == nil then
        value = ""
      elseif type(value) == "string" then
        value = "=" .. value
      end
      if string.match(newurl, "[%?&]" .. param .. "[=&]") then
        newurl = string.gsub(newurl, "([%?&]" .. param .. ")=?[^%?&;]*", "%1" .. value)
      else
        if string.match(newurl, "%?") then
          newurl = newurl .. "&"
        else
          newurl = newurl .. "?"
        end
        newurl = newurl .. param .. value
      end
    end
    return newurl
  end

  local function increment_param(newurl, param, default, step)
    local value = string.match(newurl, "[%?&]" .. param .. "=([0-9]+)")
    if value then
      value = tonumber(value)
      value = value + step
      return set_new_params(newurl, {[param]=tostring(value)})
    else
      if default ~= nil then
        default = tostring(default)
      end
      return set_new_params(newurl, {[param]=default})
    end
  end

  local function get_count(data)
    local count = 0
    for _ in pairs(data) do
      count = count + 1
    end 
    return count
  end

  local function extract_from_api(json, ws)
    local new_item = nil
    local workspace = (json["workspace"] and json["workspace"]["slug"]) or ws
    if json["type"] == "repository" then
      if workspace then
        local slug = json["slug"] or string.match(json["full_name"], "^([^/]+)/")
        new_item = "repo:" .. workspace .. "/" .. slug
      end
    elseif json["type"] == "team" then
      new_item = "team:" .. json["username"]
    elseif json["type"] == "workspace" then
      new_item = "workspace:" .. json["slug"]
    elseif json["type"] == "project" then
      if workspace then
        new_item = "project:" .. workspace .. ":" .. json["key"]
      end
    end
    if new_item then
      discover_item(discovered_stash_items, new_item)
    end
    for k, v in pairs(json) do
      if type(v) == "table" then
        extract_from_api(v, workspace)
      end
    end
  end

  local function check_post(url, d)
    post_data = d
    check(url)
    post_data = nil
  end

  local function git_line(s)
    return string.format("%04x", string.len(s)+4) .. s
  end

  local function contains(t, val)
    for _, v in pairs(t) do
      if v == val then
        return true
      end
    end
    return false
  end

  if allowed(url)
    and status_code < 300
    and item_type ~= "asset" then
    html = read_file(file)
    if string.match(url, "^https?://api%.bitbucket%.org/2%.0/")
      or string.match(url, "^https?://api%.bitbucket%.org/!api/2%.0/") then
      json = cjson.decode(html)
      extract_from_api(json)
      if json["values"] then
        if type(json["values"][1]) == "string"
          and string.match(json["values"][1], "Upgrade to a Standard or Premium plan") then
          return urls
        end
        if not string.match(url, "/snippets")
          and not string.match(url, "/commits")
          and not string.match(url, "/activity") then
          local count = get_count(json["values"])
          assert(
            count == json["size"]
            or count == json["pagelen"]
            or (json["size"] and json["pagelen"] and count == (json["size"] % json["pagelen"]))
          )
        end
      end
      if json["next"] then
        check(json["next"])
      end
    end

    -- repo
    if item_type == "repo" then
      if not context["state"]
        and (
          string.match(url, "^https?://[^/]*bitbucket%.org/([^/]+/[^/]+)$") == item_value
          or string.match(url, "^https?://[^/]*bitbucket%.org/([^/]+/[^/]+)/src$") == item_value
          or string.match(url, "^https?://[^/]*bitbucket%.org/([^/]+/[^/]+)/src/master$") == item_value
        ) then
        context["state"] = cjson.decode(string.match(html, "window%.__initial_state__%s*=%s*({.-});"))
        context["hash"] = context["state"]["repository"]["source"]["section"]["hash"]
        context["user_uuid"] = context["state"]["global"]["targetUser"]["uuid"]
        check("https://bitbucket.org/" .. item_value .. "/src/" .. context["state"]["repository"]["source"]["section"]["ref"]["name"])
      end
      check("https://bitbucket.org/" .. item_value .. ".git/info/refs?service=git-upload-pack")
      if context["hash"] then
      for base, paths in pairs({
        ["https://bitbucket.org/" .. item_value .. "/"] = {
          "",
          "src",
          "src?state=view",
          "src/master",
          "commits/",
          "branches/",
          "pull-requests/",
          "packages/",
          "packages/containers/",
          "pipelines/",
          "deployments/",
          "jira/",
          "jira/?state=view",
          "security/",
          "security/?state=view",
          "downloads/",
          "downloads/?iframe=true&spa=0",
          "downloads/?tab=tags",
          "downloads/?tab=tags&_pjax=%23downloads%20.downloads-pjax-container",
          "downloads/?tab=downloads",
          "downloads/?tab=downloads&_pjax=%23downloads%20.downloads-pjax-container",
          "downloads/?tab=branches",
          "downloads/?tab=branches&_pjax=%23downloads%20.downloads-pjax-container"
        },
        ["https://bitbucket.org/!api/2.0/repositories/" .. item_value .. "/"] = {
          "effective-branching-model",
          "pipelines/?page=1&pagelen=1&sort=-created_on&fields=%2Bvalues.target.commit.message%2C%2Bvalues.target.selector.type%2Bvalues.target.selector.pattern%2Bvalues.target.commit.summary.html%2C%2Bvalues.target.%2A%2C%2Bvalues.%2A%2C%2Bpage%2C%2Bsize",
          "pipelines/?page=1&pagelen=20&sort=-created_on&fields=%2Bvalues.target.commit.message%2C%2Bvalues.target.selector.type%2Bvalues.target.selector.pattern%2Bvalues.target.commit.summary.html%2C%2Bvalues.target.%2A%2C%2Bvalues.%2A%2C%2Bpage%2C%2Bsize",
          "pipelines/?page=1&pagelen=100&sort=-created_on&fields=-values.%2A%2C%2Bvalues.creator%2C%2Bvalues.trigger",
          "pipelines_config/variables/?page=1&pagelen=100",
          "refs/branches/master",
          "refs/branches?pagelen=25&q=name%20%21%3D%20%22master%22%20AND%20name%20~%20%22%22&sort=name&fields=-values.target.%2A%2C%2Bvalues.target.hash",
          "refs/tags?pagelen=25&q=name%20~%20%22%22&sort=-target.date&fields=-values.target.%2A%2C%2Bvalues.target.hash",
          --"src/master/bitbucket-pipelines.yml",
          "pullrequests?pagelen=25&fields=%2Bvalues.participants%2C-values.description%2C-values.summary&q=state%3D%22OPEN%22%20AND%20queued%3Dfalse&page=1"
        },
        ["https://bitbucket.org/!api/internal/menu/repository/" .. item_value] = {
          "?fields=%2Brepository.landing_page",
        },
        ["https://bitbucket.org/!api/internal/repositories/" .. item_value .. "/"] = {
          "details?fields=%2Bparent.mainbranch.%2A",
          "tree/" .. context["hash"] .. "/?max_depth=1&no_size=1",
          "tree/" .. context["hash"] .. "/?no_size=1",
          "tree/master/?no_size=1",
          "metadata",
          "srcdir-with-metadata/" .. context["hash"] .. "/",
          "changesets?fields=%2B%2A.committer.%2A%2C%2B%2A.participants.approved%2C-%2A.participants.%2A&include=master&page=1&pagelen=25&truncate_to=1024",
          "changesets?fields=%2B%2A.committer.%2A%2C%2B%2A.participants.approved%2C-%2A.participants.%2A&include=master&ctx=6a3b694c029da5ef562d4f49d3ce98cd&page=2&pagelen=25&truncate_to=1024",
          "branch-list/?branch=master&fields=-values.target.author.user.account_id%2C%2Bvalues.pullrequest.state%2C%2Bvalues.pullrequest.created_on%2C%2Bvalues.pullrequests.state%2C%2Bvalues.pullrequests.created_on%2C%2Bvalues.pullrequests.closed_on%2C-values.statuses%2C%2Bvalues.default_merge_strategy%2C%2Bvalues.merge_strategies%2C%2Bvalues.sync_strategies",
          "branch-list/?q=name%20%21%3D%20%22master%22%20AND%20%28ahead%20%3E%200%20OR%20ahead%20%3D%20null%29&pagelen=25&fields=-values.target.author.user.account_id%2C%2Bvalues.pullrequest.state%2C%2Bvalues.pullrequest.created_on%2C%2Bvalues.pullrequests.state%2C%2Bvalues.pullrequests.created_on%2C%2Bvalues.pullrequests.closed_on%2C-values.statuses%2C%2Bvalues.default_merge_strategy%2C%2Bvalues.merge_strategies%2C%2Bvalues.sync_strategies",
          "pr-authors/?pr_status=open&pr_status=draft",
          --"branch-restrictions/group-by-branch/",
          "environment-dashboard?includeHidden=false&fields=%2Bgroupings.environment_summaries.latest_successful_deployment.deployable.commit.%2A.%2A%2C-groupings.environment_summaries.latest_successful_deployment.deployable.commit.parents%2C-groupings.environment_summaries.latest_successful_deployment.deployable.commit.links.patch%2C-groupings.environment_summaries.latest_successful_deployment.deployable.commit.properties%2C%2Bgroupings.environment_summaries.latest_successful_deployment.step.run_number%2C%2Bgroupings.environment_summaries.latest_successful_deployment.step.state.%2A.%2A%2C%2Bgroupings.environment_summaries.latest_successful_deployment.step.trigger.%2A.%2A%2C%2Bgroupings.environment_summaries.latest_deployment.deployable.commit.%2A.%2A%2C-groupings.environment_summaries.latest_deployment.deployable.commit.parents%2C-groupings.environment_summaries.latest_deployment.deployable.commit.links.patch%2C-groupings.environment_summaries.latest_deployment.deployable.commit.properties%2C%2Bgroupings.environment_summaries.latest_deployment.step.run_number%2C%2Bgroupings.environment_summaries.latest_deployment.step.state.%2A.%2A%2C%2Bgroupings.environment_summaries.latest_deployment.step.trigger.%2A.%2A%2C%2Bgroupings.environment_summaries.next_promotion.environment.name",
          --"jira/sites",
          --"jira/projects?page=1",
          --"packages/containers?pagelen=20",
        }
      }) do
        for _, path in pairs(paths) do
          check(base .. path)
        end
      end
      check_post(
        "https://bitbucket.org/!api/internal/connect/modules/%7B" .. context["user_uuid"] .. "%7D",
        '[{"target":{"type":"repository","full_name":"' .. item_value .. '","name":"' .. context["name"] .. '","links":{"self":{"href":"https://bitbucket.org/!api/2.0/repositories/' .. item_value .. '"}}},"modules":["webPanels:org.bitbucket.repository.overview.informationPanel","generalPages"]}]'
      )
      check_post(
        "https://bitbucket.org/!api/internal/connect/modules/%7B" .. context["user_uuid"] .. "%7D",
        '[{"target":{"type":"repository","full_name":"' .. item_value .. '","name":"' .. context["name"] .. '","links":{"self":{"href":"https://bitbucket.org/!api/2.0/repositories/' .. item_value .. '"}}},"modules":["webCards:org.bitbucket.source"]}]'
      )
    end
    end
    if string.match(url, "%.git/info/refs%?service=git%-upload%-pack$") then
      check_post(
        string.match(url, "^(.-%.git)/info") .. "/git-upload-pack",
        git_line("command=ls-refs\n")
        .. git_line("agent=git/2.30.2\n")
        .. git_line("object-format=sha1")
        .. "0001"
        .. git_line("peel\n")
        .. git_line("symrefs\n")
        .. git_line("ref-prefix HEAD\n")
        .. git_line("ref-prefix refs/\n")
        .. git_line("ref-prefix refs/tags/")
        .. "0000"
      )
    end
    if string.match(url, "%.git/git%-upload%-pack$")
      and not string.match(html, "^000dpackfile") then
      local post =
        git_line("command=fetch\n")
        .. git_line("agent=git/2.30.2\n")
        .. git_line("object-format=sha1")
        .. "0001"
        .. git_line("thin-pack")
        .. git_line("no-progress")
        .. git_line("ofs-delta")
      local function read_n(s, n)
        local a = string.sub(s, 1, n)
        assert(string.len(a) == n)
        local b = string.sub(s, n+1)
        return a, b
      end
      local prev = nil
      local temp = html
      local data = nil
      while true do
        if prev then
          post = post .. git_line(prev .. "\n")
          prev = nil
        end
        data, temp = read_n(temp, 4)
        data = tonumber(data, 16)
        if data == 0 then
          if prev then
            post = post .. git_line(prev)
            prev = nil
          end
          break
        elseif data >= 4 then
          data, temp = read_n(temp, data-4)
          local sha1, name = string.match(data, "^([^ ]+) (.+)$")
          if not sha1 or not name then
            error("Could not extract info.")
          end
          if string.len(sha1) ~= 40 then
            error("Incorrect hash length " .. tostring(string.len(sha1)) .. ".")
          end
          prev = "want " .. sha1
        end
      end
      if prev ~= nil then
        error("Not all hashes consumed...")
      end
      post = post
        .. git_line("done")
        .. "0000"
      check_post(url, post)
    end

    -- workspace
    if string.match(url, "^https?://api%.bitbucket%.org/2%.0/repositories%?after=") then
      if json["next"] then
        check(json["next"])
      end
    end
    if string.match(url, "^https?://[^/]*bitbucket%.org/!api/internal/workspaces/[^/]+/pullrequests/") then
      local commit_list = {}
      for _, pr in pairs(json["values"]) do
        local hash = pr["source"] and pr["source"]["commit"] and pr["source"]["commit"]["hash"]
        if hash and not contains(commit_list, hash) then
          table.insert(commit_list, hash)
          if #commit_list == 25 then
            break
          end
        end
      end
      if #commit_list > 0 then
        check_post(
          "https://bitbucket.org/!api/internal/workspaces/" .. item_value .. "/commits/statuses",
          cjson.encode({["commits"]=commit_list})
        )
      end
    end
    if string.match(url, "^https?://api%.bitbucket%.org/2%.0/workspaces/[^/]+$") then
      context["uuid"] = string.match(json["uuid"], "^{(.+)}$")
      ids[context["uuid"]] = true
      assert(context["uuid"])
      check("https://api.bitbucket.org/2.0/workspaces/" .. json["uuid"])
      check("https://api.bitbucket.org/2.0/workspaces/" .. item_value .. "/projects")
      check("https://api.bitbucket.org/2.0/repositories/" .. item_value)
      check("https://api.bitbucket.org/2.0/snippets/" .. item_value)
      if item_type == "workspace" then
        check(json["links"]["avatar"]["href"])
        check("https://bitbucket.org/workspaces/" .. item_value .. "/avatar/")
      end
      return urls
    end
    if item_type == "workspace-check" and (
      string.match(url, "/2%.0/workspaces/")
      or string.match(url, "/2%.0/repositories/")
      or string.match(url, "/2%.0/snippets/")
    ) then
      if not context["recent_update"] then
        for _, d in pairs(json["values"]) do
          for _, k in pairs({"created_on", "updated_on"}) do
            if d[k] then
              local yearmonth = string.match(d[k], "^([0-9][0-9][0-9][0-9]%-[0-9][0-9])")
              if yearmonth > "2025-08" then
                print("Found a recent update at " .. yearmonth .. "!")
                context["recent_update"] = true
              end
            end
          end
        end
      end
    end
    if item_type == "workspace" then
      check("https://bitbucket.org/" .. item_value .. "/")
      check("https://bitbucket.org/" .. item_value .. "/workspace/overview")
      check("https://bitbucket.org/" .. item_value .. "/workspace/repositories/")
      check("https://bitbucket.org/" .. item_value .. "/workspace/projects/")
      check("https://bitbucket.org/" .. item_value .. "/workspace/pull-requests/")
    end
    if string.match(url, "^https?://[^/]+/[^/]+/workspace/repositories/$") then
      check_post(
        "https://api.atlassian.com/flags/api/v2/frontend/experimentValues",
        cjson.encode({
          ["identifiers"] = {
            ["bitbucketWorkspaceId"] = context["uuid"]
          },
          ["customAttributes"] = {
            ["atlassian_staff"] = false,
            ["bitbucket_team"] = false
          },
          ["targetApp"] = "bitbucket-cloud_web"
        })
      )
      check("https://bitbucket.org/!api/internal/repositories/" .. item_value .. "/languages")
      check("https://bitbucket.org/!api/internal/menu/workspace/" .. item_value)
      check("https://bitbucket.org/!api/2.0/repositories/" .. item_value .. "?page=1&pagelen=25&sort=-updated_on&q=&fields=-values.owner%2C-values.workspace")
      check("https://bitbucket.org/!api/2.0/workspaces/" .. item_value .. "/projects/?sort=name&fields=-values.owner%2C-values.workspace")
    end
    if string.match(url, "^https?://[^/]+/[^/]+/workspace/pull%-requests/$") then
      check("https://bitbucket.org/" .. item_value .. "/workspace/overview/?state=section")
      check("https://bitbucket.org/!api/internal/workspaces/" .. item_value .. "/pullrequests/?fields=-values.closed_by%2C-values.description%2C-values.summary%2C-values.rendered%2C-values.properties%2C-values.reason%2C-values.reviewers%2C-values.participants.user.nickname%2C%2Bvalues.destination.branch.name%2C%2Bvalues.destination.repository.full_name%2C%2Bvalues.destination.repository.name%2C%2Bvalues.destination.repository.uuid%2C%2Bvalues.destination.repository.full_name%2C%2Bvalues.destination.repository.name%2C%2Bvalues.destination.repository.links.self.href%2C%2Bvalues.destination.repository.links.html.href%2C%2Bvalues.source.branch.name%2C%2Bvalues.source.repository.full_name%2C%2Bvalues.source.repository.name%2C%2Bvalues.source.repository.uuid%2C%2Bvalues.source.repository.full_name%2C%2Bvalues.source.repository.name%2C%2Bvalues.source.repository.links.self.href%2C%2Bvalues.source.repository.links.html.href%2C%2Bvalues.source.commit.hash&page=1&pagelen=20&q=%28%28%20state%3D%22OPEN%22%20AND%20draft%3Dfalse%20%29%20OR%20%28%20state%3D%22OPEN%22%20AND%20draft%3Dtrue%20%29%29")
      check("https://bitbucket.org/!api/internal/workspaces/" .. item_value .. "/pullrequests/?fields=-values.closed_by%2C-values.description%2C-values.summary%2C-values.rendered%2C-values.properties%2C-values.reason%2C-values.reviewers%2C-values.participants.user.nickname%2C%2Bvalues.destination.branch.name%2C%2Bvalues.destination.repository.full_name%2C%2Bvalues.destination.repository.name%2C%2Bvalues.destination.repository.uuid%2C%2Bvalues.destination.repository.full_name%2C%2Bvalues.destination.repository.name%2C%2Bvalues.destination.repository.links.self.href%2C%2Bvalues.destination.repository.links.html.href%2C%2Bvalues.source.branch.name%2C%2Bvalues.source.repository.full_name%2C%2Bvalues.source.repository.name%2C%2Bvalues.source.repository.uuid%2C%2Bvalues.source.repository.full_name%2C%2Bvalues.source.repository.name%2C%2Bvalues.source.repository.links.self.href%2C%2Bvalues.source.repository.links.html.href%2C%2Bvalues.source.commit.hash&page=1&pagelen=20&q=")
      check("https://bitbucket.org/!api/2.0/workspaces/" .. item_value .. "/projects/?page=1&q=&sort=name&fields=-values.workspace%2C-values.owner")
    end
    if string.match(url, "/!api/2%.0/repositories/[^/%?]+%?")
      or string.match(url, "/!api/internal/workspaces/[^/]+/pullrequests/%?")
      or string.match(url, "/!api/2%.0/workspaces/[^/]+/projects/%?") then
      local page = string.match(url, "[%?&]page=([0-9]+)")
      if page then
        local path = string.match(url, "/!api/2%.0/([^/]+)")
          or string.match(url, "/workspaces/[^/]+/([^/]+)/")
        if path == "repositories"
          or path == "projects" then
          check("https://bitbucket.org/" .. item_value .. "/workspace/" .. path .. "/?page=" .. page)
        elseif path == "pullrequests" then
          local q = string.match(url, "[%?&]q=([^&]*)")
          local state = ({
            [""] = "ALL",
            ["%28%28%20state%3D%22OPEN%22%20AND%20draft%3Dfalse%20%29%20OR%20%28%20state%3D%22OPEN%22%20AND%20draft%3Dtrue%20%29%29)"] = "DRAFT%2BOPEN",
          })[q]
          local newurl = "https://bitbucket.org/" .. item_value .. "/workspace/pull-requests/?state=" .. state
          check(newurl)
          check(newurl .. "&page=" .. page)
        end
      end
    end
    if item_type == "workspace-check"
      or string.match(url, "%.git/") then
      return urls
    end
    for newurl in string.gmatch(string.gsub(html, "&[qQ][uU][oO][tT];", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    html = string.gsub(html, "&gt;", ">")
    html = string.gsub(html, "&lt;", "<")
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  is_initial_url = false
  if http_stat["statcode"] ~= 200
    and http_stat["statcode"] ~= 301
    and http_stat["statcode"] ~= 302 then
    retry_url = true
    return false
  end
  if http_stat["len"] == 0
    and http_stat["statcode"] < 300 then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    local maxtries = 11
    if status_code == 401 or status_code == 403 then
      tries = maxtries + 1
    end
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    if status_code == 200 then
      if not seen_200[url["url"]] then
        seen_200[url["url"]] = 0
      end
      seen_200[url["url"]] = seen_200[url["url"]] + 1
    end
    downloaded[url["url"]] = true
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  finalize_item()

  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 5
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["bitbucket2-stash-dcysax2j04ubqz0m?shard=stashdisco"] = discovered_stash_items,
    ["bitbucket2-gs2osbgwc5breiqj"] = discovered_items,
    ["urls-4e61n66e8tkc1gzz"] = discovered_outlinks
  }) do
    print("queuing for", string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 1000 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end


