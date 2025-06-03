package com.evoura.snowplow.model;

import java.io.Serializable;

public class Enrichment implements Serializable {
  public String platform;
  public Marketing marketing;
  public User user;

  public Enrichment() {}

  public Enrichment(String platform, Marketing marketing, User user) {
    this.platform = platform;
    this.marketing = marketing;
    this.user = user;
  }

  public static class Marketing implements Serializable {
    public String campaign;
    public String source;
    public String medium;
    public String term;
    public String content;

    public Marketing() {}

    public Marketing(String campaign, String source, String medium, String term, String content) {
      this.campaign = campaign;
      this.source = source;
      this.medium = medium;
      this.term = term;
      this.content = content;
    }
  }

  public static class User implements Serializable {
    public String ipAddress;

    public User() {}

    public User(String ipAddress) {
      this.ipAddress = ipAddress;
    }
  }
}
