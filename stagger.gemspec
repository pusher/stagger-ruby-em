# coding: utf-8

Gem::Specification.new do |spec|
  spec.name          = "stagger"
  spec.version       = "0.0.1"
  spec.authors       = ["Martyn Loughran"]
  spec.email         = ["me@mloughran.com"]
  spec.description   = %q{Stagger client for eventmachine}
  spec.summary       = %q{Stagger client for eventmachine}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency "eventmachine"
  spec.add_dependency "msgpack"

  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rake"
end
