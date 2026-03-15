require "../spec_helper"

Spectator.describe Redis::GlobMatcher do
  def matches(pattern : String, value : String) : Bool
    Redis::GlobMatcher.compile(pattern).matches?(b(value))
  end

  it "matches exact strings" do
    expect(matches("hello", "hello")).to be_true
    expect(matches("hello", "hella")).to be_false
  end

  it "matches wildcard prefixes" do
    expect(matches("user:*", "user:1")).to be_true
    expect(matches("user:*", "post:1")).to be_false
  end

  it "matches single-character wildcards" do
    expect(matches("h?llo", "hello")).to be_true
    expect(matches("h?llo", "hallo")).to be_true
    expect(matches("h?llo", "hllo")).to be_false
  end

  it "matches character classes" do
    expect(matches("h[ae]llo", "hello")).to be_true
    expect(matches("h[ae]llo", "hallo")).to be_true
    expect(matches("h[ae]llo", "hxllo")).to be_false
  end

  it "matches negated character classes" do
    expect(matches("h[^e]llo", "hallo")).to be_true
    expect(matches("h[^e]llo", "hello")).to be_false
  end

  it "matches ranges" do
    expect(matches("h[a-z]llo", "hxllo")).to be_true
    expect(matches("h[a-b]llo", "hallo")).to be_true
    expect(matches("h[a-b]llo", "hello")).to be_false
  end

  it "matches escaped special characters literally" do
    expect(matches("h\\?llo", "h?llo")).to be_true
    expect(matches("h\\*llo", "h*llo")).to be_true
    expect(matches("h\\[llo", "h[llo")).to be_true
  end
end
