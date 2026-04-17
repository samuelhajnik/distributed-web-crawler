import { describe, expect, it } from "vitest";
import { buildAllowedHostSet, normalizeAbsoluteUrl, normalizeUrl, parseSeedUrl } from "./url";

describe("buildAllowedHostSet", () => {
  it("pairs apex seed with www host", () => {
    expect([...buildAllowedHostSet("Example.COM")].sort()).toEqual(["example.com", "www.example.com"].sort());
  });

  it("pairs www seed with apex host", () => {
    expect([...buildAllowedHostSet("www.example.com")].sort()).toEqual(["example.com", "www.example.com"].sort());
  });
});

describe("parseSeedUrl", () => {
  it("normalizes seed and derives scope for apex", () => {
    const r = parseSeedUrl("  https://example.com/path#x  ");
    expect(r?.normalized).toBe("https://example.com/path");
    expect(r?.hostname).toBe("example.com");
    expect([...r!.allowedHosts].sort()).toEqual(["example.com", "www.example.com"].sort());
  });

  it("normalizes seed and derives scope for www", () => {
    const r = parseSeedUrl("https://www.example.org/");
    expect(r?.hostname).toBe("www.example.org");
    expect([...r!.allowedHosts].sort()).toEqual(["example.org", "www.example.org"].sort());
  });

  it("rejects non-http(s) schemes", () => {
    expect(parseSeedUrl("ftp://example.com/")).toBeNull();
  });

  it("rejects empty input", () => {
    expect(parseSeedUrl("")).toBeNull();
    expect(parseSeedUrl("   ")).toBeNull();
  });
});

describe("normalizeAbsoluteUrl", () => {
  it("strips hash and default https port", () => {
    expect(normalizeAbsoluteUrl("https://docs.test:443/x#y")).toBe("https://docs.test/x");
  });
});

describe("normalizeUrl", () => {
  const apexScope = buildAllowedHostSet("example.com");

  it("resolves relative links on allowed hosts", () => {
    expect(normalizeUrl("https://example.com/", "https://example.com/page#x", apexScope)).toBe("https://example.com/page");
    expect(normalizeUrl("https://example.com/docs/foo", "bar", apexScope)).toBe("https://example.com/docs/bar");
  });

  it("rejects hosts outside crawl scope", () => {
    expect(normalizeUrl("https://example.com/", "https://evil.com/x", apexScope)).toBeNull();
    expect(normalizeUrl("https://example.com/", "https://cdn.example.com/x", apexScope)).toBeNull();
  });

  it("allows www counterpart when seed was apex", () => {
    expect(normalizeUrl("https://example.com/", "https://www.example.com/a", apexScope)).toBe("https://www.example.com/a");
  });

  it("allows apex when seed was www", () => {
    const wwwScope = buildAllowedHostSet("www.example.com");
    expect(normalizeUrl("https://www.example.com/", "/b", wwwScope)).toBe("https://www.example.com/b");
    expect(normalizeUrl("https://www.example.com/", "https://example.com/c", wwwScope)).toBe("https://example.com/c");
  });

  it("filters non-http schemes", () => {
    expect(normalizeUrl("https://example.com/", "mailto:a@b.com", apexScope)).toBeNull();
    expect(normalizeUrl("https://example.com/", "tel:+1", apexScope)).toBeNull();
    expect(normalizeUrl("https://example.com/", "javascript:void(0)", apexScope)).toBeNull();
  });

  it("preserves query strings and strips default port", () => {
    expect(normalizeUrl("https://example.com/", "https://example.com/x?a=1&b=2", apexScope)).toBe("https://example.com/x?a=1&b=2");
    expect(normalizeUrl("https://example.com/", "https://example.com:443/x", apexScope)).toBe("https://example.com/x");
  });
});
