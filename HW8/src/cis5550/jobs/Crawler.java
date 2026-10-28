package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.URLParser;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * A simple distributed web crawler, built on top of the Flame engine (HW6/HW7)
 * and the key-value store (HW4/HW5).
 *
 * The crawler keeps a "queue" of URLs that still need to be visited in an RDD,
 * and runs a flatMap over that queue in every round. Pages that have been
 * downloaded end up in the 'pt-crawl' table in the KVS.
 */
public class Crawler {

  private static final Logger logger = Logger.getLogger(Crawler.class);

  /** The table with the crawl results. */
  static final String CRAWL_TABLE = "pt-crawl";
  /** Per-host state: the cached robots.txt, and the time of the last request. */
  static final String HOSTS_TABLE = "pt-hosts";
  /** Content hash -> URL of the first page we saw with that content (EC1). */
  static final String CONTENT_TABLE = "pt-content";
  /** Staging area for the anchor texts, merged into 'pt-crawl' at the end (EC3). */
  static final String ANCHOR_TABLE = "pt-anchors";

  static final String USER_AGENT = "cis5550-crawler";

  /** The delay between requests to the same host when robots.txt doesn't say. */
  static final long DEFAULT_DELAY_MS = 1000;
  /**
   * If a URL has to wait no longer than this for its host to become available
   * again, the worker waits inline instead of putting the URL back in the queue;
   * this avoids spending an entire round on a host whose crawl delay is tiny.
   */
  static final long MAX_INLINE_WAIT_MS = 2000;

  static final int CONNECT_TIMEOUT_MS = 5000;
  static final int READ_TIMEOUT_MS = 10000;

  static final String[] EXCLUDED_EXTENSIONS = { ".jpg", ".jpeg", ".gif", ".png", ".txt" };

  private static final List<String> NOTHING = Collections.emptyList();

  /* Per-JVM caches, so that the workers don't go back to the KVS (or to the web
     server) for things that cannot change during a crawl. */

  private static final Map<String, KVSClient> kvsCache = new ConcurrentHashMap<>();
  private static final Map<String, Robots> robotsCache = new ConcurrentHashMap<>();
  private static final Map<String, List<Pattern>> blacklistCache = new ConcurrentHashMap<>();

  /* ------------------------------------------------------------------ */
  /* The job itself                                                      */
  /* ------------------------------------------------------------------ */

  public static void run(FlameContext ctx, String[] args) throws Exception {
    if (args.length < 1) {
      ctx.output("Error: the crawler needs a seed URL as its first argument");
      return;
    }

    String seed = normalizeSeedURL(args[0]);
    if (seed == null) {
      ctx.output("Error: '" + args[0] + "' is not a usable seed URL");
      return;
    }

    String blacklist = (args.length > 1) && (args[1] != null) && !args[1].isEmpty() ? args[1] : null;
    String kvsCoordinator = ctx.getKVS().getCoordinator();

    FlameRDD queue = ctx.parallelize(Arrays.asList(seed));
    int round = 0;
    while (queue.count() > 0) {
      round++;
      FlameRDD crawled = queue.flatMap(url -> crawlOneURL(kvsCoordinator, url, blacklist));
      FlameRDD next = crawled.distinct();
      crawled.destroy();
      queue.destroy();
      queue = next;
      Thread.sleep(10);
    }
    queue.destroy();

    int anchorsAdded = mergeAnchorText(ctx.getKVS());

    ctx.output("OK: crawled " + ctx.getKVS().count(CRAWL_TABLE) + " URL(s) in " + round
        + " round(s), starting from " + seed
        + (anchorsAdded > 0 ? ("; added anchor text to " + anchorsAdded + " row(s)") : ""));
  }

  /**
   * Everything that happens to a single URL in one round. Returns the URLs that
   * should be in the queue for the next round.
   */
  static Iterable<String> crawlOneURL(String kvsCoordinator, String url, String blacklistTable) {
    try {
      KVSClient kvs = kvs(kvsCoordinator);
      String urlHash = Hasher.hash(url);

      /* Have we been here before? */

      if (kvs.existsRow(CRAWL_TABLE, urlHash))
        return NOTHING;

      if (isBlacklisted(kvs, blacklistTable, url))
        return NOTHING;

      String[] parts = URLParser.parseURL(url);
      String protocol = parts[0], host = parts[1], port = parts[2], path = parts[3];
      if ((protocol == null) || (host == null) || (port == null))
        return NOTHING;
      String hostKey = host + ":" + port;

      /* The robot exclusion protocol: fetch robots.txt once per host, then see
         whether this particular URL is off limits. */

      Robots robots = robotsFor(kvs, hostKey, protocol, host, port);
      if (!robots.allows(path))
        return NOTHING;

      /* Politeness: don't hit the same host more often than we are allowed to. */

      long delay = robots.delayMillis();
      long now = System.currentTimeMillis();
      Row hostRow = kvs.getRow(HOSTS_TABLE, hostKey);
      String lastAccess = (hostRow == null) ? null : hostRow.get("lastAccess");
      if (lastAccess != null) {
        long wait = delay - (now - Long.parseLong(lastAccess));
        if (wait > 0) {
          if (wait > MAX_INLINE_WAIT_MS)
            return Collections.singletonList(url);
          Thread.sleep(wait);
          now = System.currentTimeMillis();
        }
      }
      kvs.put(HOSTS_TABLE, hostKey, "lastAccess", String.valueOf(now));

      /* The HEAD request. */

      HttpURLConnection head = openConnection(url, "HEAD");
      int responseCode;
      String contentType, contentLength, location;
      try {
        responseCode = head.getResponseCode();
        contentType = head.getHeaderField("Content-Type");
        contentLength = head.getHeaderField("Content-Length");
        location = head.getHeaderField("Location");
      } finally {
        head.disconnect();
      }

      Row row = new Row(urlHash);
      row.put("url", url);
      row.put("responseCode", String.valueOf(responseCode));
      if (contentType != null)
        row.put("contentType", contentType);
      if (contentLength != null)
        row.put("length", contentLength);

      /* A redirect: remember the code so we don't come back, and hand the new
         URL to the next round rather than following the chain right now. */

      if (isRedirect(responseCode)) {
        kvs.putRow(CRAWL_TABLE, row);
        String target = normalizeURL(url, location);
        if ((target == null) || isBlacklisted(kvs, blacklistTable, target))
          return NOTHING;
        return Collections.singletonList(target);
      }

      if ((responseCode != 200) || !isHTML(contentType)) {
        kvs.putRow(CRAWL_TABLE, row);
        return NOTHING;
      }

      /* The GET request. HEAD and GET for the *same* URL may go back-to-back,
         so the host's timestamp is only bumped once the GET is done. */

      HttpURLConnection get = openConnection(url, "GET");
      byte[] page;
      try {
        row.put("responseCode", String.valueOf(get.getResponseCode()));
        page = readFully(get);
      } finally {
        get.disconnect();
        kvs.put(HOSTS_TABLE, hostKey, "lastAccess", String.valueOf(System.currentTimeMillis()));
      }

      /* EC1: if we have seen this exact content before, point at the page we
         already have instead of storing a second copy. */

      String canonical = canonicalURLFor(kvs, page, url);
      if (canonical != null)
        row.put("canonicalURL", canonical);
      else
        row.put("page", page);
      kvs.putRow(CRAWL_TABLE, row);

      /* Extract, normalize and filter the links on the page. */

      Set<String> found = new LinkedHashSet<>();
      Map<String, StringBuilder> anchorText = new LinkedHashMap<>();
      for (String[] anchor : extractAnchors(new String(page, StandardCharsets.UTF_8))) {
        String target = normalizeURL(url, anchor[0]);
        if ((target == null) || isBlacklisted(kvs, blacklistTable, target))
          continue;
        found.add(target);
        String text = collapseWhitespace(anchor[1]);
        if (!text.isEmpty()) {
          StringBuilder sb = anchorText.computeIfAbsent(target, k -> new StringBuilder());
          if (sb.length() > 0)
            sb.append(' ');
          sb.append(text);
        }
      }

      /* EC3: stash the anchor texts; they are merged into 'pt-crawl' once the
         crawl is over, so that we never create a row for a URL that is never
         actually crawled. */

      for (Map.Entry<String, StringBuilder> e : anchorText.entrySet())
        kvs.put(ANCHOR_TABLE, Hasher.hash(e.getKey()), "anchor:" + urlHash, e.getValue().toString());

      return found;
    } catch (Exception e) {
      logger.error("Error while crawling " + url, e);
      return NOTHING;
    }
  }

  /* ------------------------------------------------------------------ */
  /* HTTP                                                                */
  /* ------------------------------------------------------------------ */

  static HttpURLConnection openConnection(String url, String method) throws Exception {
    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
    conn.setRequestMethod(method);
    conn.setInstanceFollowRedirects(false);
    conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
    conn.setReadTimeout(READ_TIMEOUT_MS);
    conn.setRequestProperty("User-Agent", USER_AGENT);
    conn.connect();
    return conn;
  }

  static byte[] readFully(HttpURLConnection conn) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    InputStream in = null;
    try {
      in = conn.getInputStream();
    } catch (Exception e) {
      in = conn.getErrorStream();
    }
    if (in == null)
      return new byte[0];
    try {
      byte[] buffer = new byte[8192];
      int n;
      while ((n = in.read(buffer)) > 0)
        out.write(buffer, 0, n);
    } catch (Exception e) {
      // Whatever we have read so far is what we get.
    } finally {
      try { in.close(); } catch (Exception e) { }
    }
    return out.toByteArray();
  }

  static boolean isRedirect(int code) {
    return (code == 301) || (code == 302) || (code == 303) || (code == 307) || (code == 308);
  }

  static boolean isHTML(String contentType) {
    if (contentType == null)
      return false;
    int semicolon = contentType.indexOf(';');
    String type = (semicolon >= 0) ? contentType.substring(0, semicolon) : contentType;
    return type.trim().equalsIgnoreCase("text/html");
  }

  static KVSClient kvs(String coordinator) {
    return kvsCache.computeIfAbsent(coordinator, KVSClient::new);
  }

  /* ------------------------------------------------------------------ */
  /* The robot exclusion protocol                                        */
  /* ------------------------------------------------------------------ */

  /**
   * Returns the (parsed) robots.txt for a host, downloading it if this is the
   * first time anyone has asked for it. There is at most one request per host.
   */
  static Robots robotsFor(KVSClient kvs, String hostKey, String protocol, String host, String port) {
    Robots cached = robotsCache.get(hostKey);
    if (cached != null)
      return cached;

    Robots robots;
    try {
      Row hostRow = kvs.getRow(HOSTS_TABLE, hostKey);
      if ((hostRow != null) && (hostRow.get("robotsFetched") != null)) {
        robots = Robots.parse(hostRow.get("robots"));
      } else {
        String body = null;
        try {
          HttpURLConnection conn = openConnection(protocol + "://" + host + ":" + port + "/robots.txt", "GET");
          try {
            if (conn.getResponseCode() == 200)
              body = new String(readFully(conn), StandardCharsets.UTF_8);
          } finally {
            conn.disconnect();
          }
        } catch (Exception e) {
          logger.warn("Could not download robots.txt from " + hostKey, e);
        }
        kvs.put(HOSTS_TABLE, hostKey, "robotsFetched", "1");
        if (body != null)
          kvs.put(HOSTS_TABLE, hostKey, "robots", body);
        robots = Robots.parse(body);
      }
    } catch (Exception e) {
      logger.error("Error while looking up robots.txt for " + hostKey, e);
      robots = Robots.parse(null);
    }

    robotsCache.put(hostKey, robots);
    return robots;
  }

  /** The rules from a robots.txt file that apply to our own user agent. */
  static class Robots {
    /** Each rule is {"allow"|"disallow", prefix}, in the order they appeared. */
    final List<String[]> rules;
    final Double crawlDelay;

    Robots(List<String[]> rules, Double crawlDelay) {
      this.rules = rules;
      this.crawlDelay = crawlDelay;
    }

    boolean allows(String path) {
      if (path == null)
        path = "/";
      for (String[] rule : rules) {
        if (rule[1].isEmpty())
          continue;
        if (path.startsWith(rule[1]))
          return rule[0].equals("allow");
      }
      return true;
    }

    long delayMillis() {
      if (crawlDelay == null)
        return DEFAULT_DELAY_MS;
      return Math.max(0, Math.round(crawlDelay * 1000.0));
    }

    static Robots parse(String body) {
      if (body == null)
        return new Robots(Collections.emptyList(), null);

      Map<String, List<String[]>> groups = new LinkedHashMap<>();
      Map<String, Double> delays = new LinkedHashMap<>();
      Set<String> current = new LinkedHashSet<>();
      boolean previousLineWasUserAgent = false;

      for (String rawLine : body.split("\r\n|\r|\n")) {
        String line = rawLine;
        int comment = line.indexOf('#');
        if (comment >= 0)
          line = line.substring(0, comment);
        line = line.trim();
        if (line.isEmpty())
          continue;
        int colon = line.indexOf(':');
        if (colon < 0)
          continue;
        String field = line.substring(0, colon).trim().toLowerCase();
        String value = line.substring(colon + 1).trim();

        if (field.equals("user-agent")) {
          if (!previousLineWasUserAgent)
            current.clear();
          current.add(value.toLowerCase());
          groups.computeIfAbsent(value.toLowerCase(), k -> new ArrayList<>());
          previousLineWasUserAgent = true;
          continue;
        }

        previousLineWasUserAgent = false;
        if (current.isEmpty())
          continue;
        for (String agent : current) {
          if (field.equals("allow") || field.equals("disallow")) {
            groups.computeIfAbsent(agent, k -> new ArrayList<>()).add(new String[] { field, value });
          } else if (field.equals("crawl-delay")) {
            try {
              delays.put(agent, Double.valueOf(value));
            } catch (NumberFormatException e) {
              // Ignore a malformed Crawl-delay.
            }
          }
        }
      }

      /* Rules for our own user agent win; if there aren't any, fall back to the
         wildcard; if there is no wildcard group either, everything is allowed. */

      String agent = null;
      if (!isEmptyGroup(groups, delays, USER_AGENT))
        agent = USER_AGENT;
      else if (!isEmptyGroup(groups, delays, "*"))
        agent = "*";
      if (agent == null)
        return new Robots(Collections.emptyList(), null);

      List<String[]> rules = groups.get(agent);
      return new Robots(rules == null ? Collections.emptyList() : rules, delays.get(agent));
    }

    private static boolean isEmptyGroup(Map<String, List<String[]>> groups, Map<String, Double> delays, String agent) {
      List<String[]> rules = groups.get(agent);
      return ((rules == null) || rules.isEmpty()) && !delays.containsKey(agent);
    }
  }

  /* ------------------------------------------------------------------ */
  /* URL extraction and normalization                                    */
  /* ------------------------------------------------------------------ */

  /**
   * Finds all the anchor tags on a page and returns, for each of them, the
   * value of its href attribute and the text between the opening and the
   * closing tag. Anchors without an href are skipped.
   */
  public static List<String[]> extractAnchors(String page) {
    List<String[]> result = new ArrayList<>();
    if (page == null)
      return result;

    int pos = 0, length = page.length();
    String openHref = null;
    StringBuilder text = new StringBuilder();

    while (pos < length) {
      int start = page.indexOf('<', pos);
      if (start < 0)
        break;

      /* Find the '>' that ends this tag, but don't be fooled by one inside a
         quoted attribute value. */

      int end = start + 1;
      char quote = 0;
      while (end < length) {
        char c = page.charAt(end);
        if (quote != 0) {
          if (c == quote)
            quote = 0;
        } else if ((c == '"') || (c == '\'')) {
          quote = c;
        } else if (c == '>') {
          break;
        }
        end++;
      }
      if (end >= length)
        break;

      if (openHref != null)
        text.append(page, pos, start);
      String inside = page.substring(start + 1, end);
      pos = end + 1;

      if (inside.startsWith("!") || inside.startsWith("?"))
        continue;

      boolean closing = inside.startsWith("/");
      String body = closing ? inside.substring(1) : inside;
      int nameEnd = 0;
      while ((nameEnd < body.length()) && !Character.isWhitespace(body.charAt(nameEnd))
          && (body.charAt(nameEnd) != '/'))
        nameEnd++;
      if (!body.substring(0, nameEnd).equalsIgnoreCase("a"))
        continue;

      if (openHref != null) {
        result.add(new String[] { openHref, text.toString() });
        openHref = null;
      }
      text.setLength(0);
      if (!closing)
        openHref = decodeEntities(attributeValue(body.substring(nameEnd), "href"));
    }

    if (openHref != null)
      result.add(new String[] { openHref, text.toString() });
    return result;
  }

  /** Extracts the URLs in the href attributes of the anchor tags on a page. */
  public static List<String> extractURLs(String page) {
    List<String> result = new ArrayList<>();
    for (String[] anchor : extractAnchors(page))
      result.add(anchor[0]);
    return result;
  }

  /**
   * Pulls one attribute out of the part of a tag that follows the tag name.
   * Handles double-quoted, single-quoted and unquoted values, and attributes
   * without a value at all.
   */
  static String attributeValue(String attributes, String wanted) {
    int pos = 0, length = attributes.length();
    while (pos < length) {
      while ((pos < length) && (Character.isWhitespace(attributes.charAt(pos)) || (attributes.charAt(pos) == '/')))
        pos++;
      int nameStart = pos;
      while ((pos < length) && !Character.isWhitespace(attributes.charAt(pos)) && (attributes.charAt(pos) != '=')
          && (attributes.charAt(pos) != '/'))
        pos++;
      String name = attributes.substring(nameStart, pos);
      if (name.isEmpty()) {
        pos++;
        continue;
      }

      int afterName = pos;
      while ((pos < length) && Character.isWhitespace(attributes.charAt(pos)))
        pos++;

      String value = null;
      if ((pos < length) && (attributes.charAt(pos) == '=')) {
        pos++;
        while ((pos < length) && Character.isWhitespace(attributes.charAt(pos)))
          pos++;
        if ((pos < length) && ((attributes.charAt(pos) == '"') || (attributes.charAt(pos) == '\''))) {
          char quote = attributes.charAt(pos++);
          int valueStart = pos;
          while ((pos < length) && (attributes.charAt(pos) != quote))
            pos++;
          value = attributes.substring(valueStart, Math.min(pos, length));
          pos++;
        } else {
          int valueStart = pos;
          while ((pos < length) && !Character.isWhitespace(attributes.charAt(pos)))
            pos++;
          value = attributes.substring(valueStart, pos);
        }
      } else {
        pos = afterName;
      }

      if (name.equalsIgnoreCase(wanted))
        return value;
    }
    return null;
  }

  static String decodeEntities(String s) {
    if ((s == null) || (s.indexOf('&') < 0))
      return s;
    return s.replace("&amp;", "&").replace("&#38;", "&").replace("&#x26;", "&");
  }

  static String collapseWhitespace(String s) {
    if (s == null)
      return "";
    return s.replaceAll("\\s+", " ").trim();
  }

  /**
   * Turns a link found on a page into an absolute, normalized URL, or returns
   * null if the link should be filtered out.
   */
  public static String normalizeURL(String baseURL, String link) {
    if (link == null)
      return null;
    link = link.trim();

    int hash = link.indexOf('#');
    if (hash >= 0)
      link = link.substring(0, hash);

    String[] base = URLParser.parseURL(baseURL);
    String baseProtocol = base[0], baseHost = base[1], basePort = base[2];
    String basePath = ((base[3] == null) || base[3].isEmpty()) ? "/" : base[3];

    /* A link that is empty after the fragment has been cut off refers to the
       page it was found on. */

    if (link.isEmpty())
      return assemble(baseProtocol, baseHost, basePort, basePath);

    /* Reject anything that is clearly not http(s) - mailto:, javascript:, ... */

    int colon = link.indexOf(':');
    int slash = link.indexOf('/');
    if ((colon > 0) && ((slash < 0) || (colon < slash))
        && link.substring(0, colon).matches("[a-zA-Z][a-zA-Z0-9+.\\-]*")) {
      String scheme = link.substring(0, colon).toLowerCase();
      if (!scheme.equals("http") && !scheme.equals("https"))
        return null;
    }

    if (link.startsWith("//"))
      link = baseProtocol + ":" + link;

    String[] parts = URLParser.parseURL(link);
    String protocol = (parts[0] != null) ? parts[0] : baseProtocol;
    String host = parts[1];
    String port = parts[2];
    String path = parts[3];

    if (host == null) {
      host = baseHost;
      if (port == null)
        port = basePort;
    }
    if ((protocol == null) || (host == null) || host.isEmpty())
      return null;

    protocol = protocol.toLowerCase();
    if (!protocol.equals("http") && !protocol.equals("https"))
      return null;

    if ((path == null) || path.isEmpty())
      path = "/";
    if (!path.startsWith("/")) {
      int lastSlash = basePath.lastIndexOf('/');
      path = ((lastSlash >= 0) ? basePath.substring(0, lastSlash + 1) : "/") + path;
    }
    path = resolveRelativePath(path);

    if ((port == null) || port.isEmpty())
      port = protocol.equals("https") ? "443" : "80";

    if (hasExcludedExtension(path))
      return null;

    return assemble(protocol, host, port, path);
  }

  /** The simplified normalization for the seed URL, which is not relative to anything. */
  public static String normalizeSeedURL(String seedURL) {
    if (seedURL == null)
      return null;
    String url = seedURL.trim();
    int hash = url.indexOf('#');
    if (hash >= 0)
      url = url.substring(0, hash);

    String[] parts = URLParser.parseURL(url);
    String protocol = parts[0], host = parts[1], port = parts[2], path = parts[3];
    if ((protocol == null) || (host == null) || host.isEmpty())
      return null;
    protocol = protocol.toLowerCase();
    if (!protocol.equals("http") && !protocol.equals("https"))
      return null;
    if ((port == null) || port.isEmpty())
      port = protocol.equals("https") ? "443" : "80";
    if ((path == null) || path.isEmpty() || !path.startsWith("/"))
      path = "/";
    return assemble(protocol, host, port, resolveRelativePath(path));
  }

  static String assemble(String protocol, String host, String port, String path) {
    return protocol + "://" + host + ":" + port + path;
  }

  /** Removes the '.' and '..' segments from a path. */
  static String resolveRelativePath(String path) {
    String suffix = "";
    int query = path.indexOf('?');
    if (query >= 0) {
      suffix = path.substring(query);
      path = path.substring(0, query);
    }

    boolean trailingSlash = path.endsWith("/");
    Deque<String> segments = new ArrayDeque<>();
    for (String segment : path.split("/", -1)) {
      if (segment.isEmpty() || segment.equals("."))
        continue;
      if (segment.equals("..")) {
        if (!segments.isEmpty())
          segments.pollLast();
      } else {
        segments.addLast(segment);
      }
    }

    StringBuilder result = new StringBuilder();
    for (String segment : segments)
      result.append('/').append(segment);
    if (result.length() == 0)
      result.append('/');
    else if (trailingSlash)
      result.append('/');
    return result + suffix;
  }

  static boolean hasExcludedExtension(String path) {
    String p = path;
    int query = p.indexOf('?');
    if (query >= 0)
      p = p.substring(0, query);
    p = p.toLowerCase();
    for (String extension : EXCLUDED_EXTENSIONS)
      if (p.endsWith(extension))
        return true;
    return false;
  }

  /* ------------------------------------------------------------------ */
  /* EC1: the content-seen test                                          */
  /* ------------------------------------------------------------------ */

  /**
   * Returns the URL of a page that has already been crawled and has exactly the
   * same content as the one we just downloaded, or null if this content is new.
   */
  static String canonicalURLFor(KVSClient kvs, byte[] page, String url) throws Exception {
    String contentHash = sha256(page);
    Row existing = kvs.getRow(CONTENT_TABLE, contentHash);
    String canonical = (existing == null) ? null : existing.get("url");

    /* A URL from an earlier crawl whose row is gone is not a duplicate; in that
       case this page becomes the new canonical one. */

    if ((canonical != null) && !canonical.equals(url) && kvs.existsRow(CRAWL_TABLE, Hasher.hash(canonical)))
      return canonical;

    kvs.put(CONTENT_TABLE, contentHash, "url", url);
    return null;
  }

  static String sha256(byte[] data) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    StringBuilder result = new StringBuilder();
    for (byte b : digest.digest(data))
      result.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
    return result.toString();
  }

  /* ------------------------------------------------------------------ */
  /* EC2: the blacklist                                                  */
  /* ------------------------------------------------------------------ */

  static boolean isBlacklisted(KVSClient kvs, String blacklistTable, String url) {
    if (blacklistTable == null)
      return false;
    for (Pattern pattern : blacklist(kvs, blacklistTable))
      if (pattern.matcher(url).matches())
        return true;
    return false;
  }

  static List<Pattern> blacklist(KVSClient kvs, String blacklistTable) {
    return blacklistCache.computeIfAbsent(blacklistTable, table -> {
      List<Pattern> patterns = new ArrayList<>();
      try {
        Iterator<Row> rows = kvs.scan(table, null, null);
        while (rows.hasNext()) {
          String pattern = rows.next().get("pattern");
          if ((pattern != null) && !pattern.isEmpty())
            patterns.add(Pattern.compile(wildcardToRegex(pattern)));
        }
      } catch (Exception e) {
        logger.warn("Could not read the blacklist table '" + table + "'", e);
      }
      return patterns;
    });
  }

  /** Turns a blacklist pattern, in which '*' matches any sequence, into a regex. */
  static String wildcardToRegex(String pattern) {
    StringBuilder regex = new StringBuilder();
    int pos = 0;
    while (true) {
      int star = pattern.indexOf('*', pos);
      if (star < 0) {
        regex.append(Pattern.quote(pattern.substring(pos)));
        break;
      }
      if (star > pos)
        regex.append(Pattern.quote(pattern.substring(pos, star)));
      regex.append(".*");
      pos = star + 1;
    }
    return regex.toString();
  }

  /* ------------------------------------------------------------------ */
  /* EC3: anchor text                                                    */
  /* ------------------------------------------------------------------ */

  /**
   * Copies the anchor texts we collected during the crawl into the rows of the
   * pages they point at. Rows that were never crawled are left alone, so that
   * the crawl table never gains an entry for a page we did not visit.
   */
  static int mergeAnchorText(KVSClient kvs) {
    int rowsUpdated = 0;
    try {
      Iterator<Row> rows = kvs.scan(ANCHOR_TABLE, null, null);
      while (rows.hasNext()) {
        Row row = rows.next();
        if (!kvs.existsRow(CRAWL_TABLE, row.key()))
          continue;
        for (String column : row.columns())
          kvs.put(CRAWL_TABLE, row.key(), column, row.get(column));
        rowsUpdated++;
      }
    } catch (Exception e) {
      logger.warn("Could not merge the anchor text into " + CRAWL_TABLE, e);
    }
    return rowsUpdated;
  }
}
