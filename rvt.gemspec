$:.push File.expand_path("../lib", __FILE__)

require "rvt/version"

Gem::Specification.new do |s|
  s.name     = 'rvt'
  s.version  = RVT::VERSION
  s.authors  = ['Genadi Samokovarov', 'Guillermo Iguaran']
  s.email    = ['gsamokovarov@gmail.com', 'guilleiguaran@gmail.com']
  s.homepage = 'https://github.com/gsamokovarov/rvt'
  s.summary  = 'A VT100 compatible terminal, running on Rails.'

  s.license  = 'MIT'

  s.files      = Dir['{app,config,db,lib,vendor}/**/*', 'MIT-LICENSE', 'Rakefile', 'README.markdown']
  s.test_files = Dir['test/**/*']

  rails_version = '>= 4.0'

  s.add_dependency 'railties',    rails_version
  s.add_dependency 'activemodel', rails_version
  s.add_dependency 'coffee-rails'
  s.add_dependency 'sprockets-rails'
  s.add_dependency 'sqlite3'
end
