require "./spec_helper"

Spectator.describe Facet do
  it "has a version" do
    expect(Facet::VERSION).to eq("0.1.0")
  end
end
