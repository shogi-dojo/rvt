require 'sqlite3'
require 'active_model'

module RVT
  # Manage and persist (in SQLite3) RVT::Slave instances.
  class ConsoleSession
    include ActiveModel::Model

    # SQLite3 database configuration and setup
    DB_PATH = File.join(Dir.pwd, 'console_sessions.db')

    class << self
      def database
        @database ||= begin
          db = SQLite3::Database.new(DB_PATH)
          db.results_as_hash = true
          setup_schema(db)
          db
        end
      end

      private def setup_schema(db)
        db.execute(<<-SQL)
          CREATE TABLE IF NOT EXISTS console_sessions (
            pid INTEGER PRIMARY KEY,
            uid TEXT NOT NULL,
            slave_data BLOB NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
          )
        SQL
      end
    end

    # Base error class for ConsoleSession specific exceptions.
    class Error < StandardError
      def as_json(*)
        { error: to_s }
      end
    end

    # Raised when trying to find a session that is no longer in the database
    # or when the slave process exited.
    Unavailable = Class.new(Error)

    # Raised when an operation transition to an invalid state.
    Invalid = Class.new(Error)

    # Raised when a request doesn't know the slave process uid.
    Unauthorized = Class.new(Error)

    class << self
      # Finds a session by its pid.
      #
      # Raises RVT::ConsoleSession::Unavailable if there is no such session.
      def find(pid)
        row = database.get_first_row('SELECT * FROM console_sessions WHERE pid = ?', pid.to_i)
        raise Unavailable, 'Session unavailable' unless row

        new.tap do |session|
          session.instance_variable_set(:@slave, Marshal.load(row['slave_data']))
          session.instance_variable_set(:@uid, row['uid'])
        end
      end

      # Finds a session by its pid and uid.
      #
      # Raises RVT::ConsoleSession::Unavailable if there is no such session.
      # Raises RVT::ConsoleSession::Unauthorized if uid doesn't match.
      def find_by_pid_and_uid(pid, uid)
        find(pid).tap do |console_session|
          raise Unauthorized if console_session.uid != uid
        end
      end

      # Creates an already persisted console session.
      def create
        new.persist
      end

      # Cleanup old sessions (optional, can be called periodically)
      def cleanup_old_sessions(hours_old = 24)
        database.execute(
          'DELETE FROM console_sessions WHERE created_at < datetime("now", "-? hours")',
          hours_old
        )
      end
    end

    def initialize
      @slave = Slave.new
      @uid = SecureRandom.uuid
    end

    # Explicitly persist the model in SQLite3.
    def persist
      self.class.database.execute(
        'INSERT OR REPLACE INTO console_sessions (pid, uid, slave_data) VALUES (?, ?, ?)',
        [pid, uid, Marshal.dump(@slave)]
      )
      self
    end

    # Returns true if the current session is persisted in SQLite3.
    def persisted?
      return false unless pid

      self.class.database.get_first_value(
        'SELECT 1 FROM console_sessions WHERE pid = ?',
        pid
      )
    end

    # Returns an Enumerable of all key attributes if any is set, regardless if
    # the object is persisted or not.
    def to_key
      [pid] if persisted?
    end

    attr_reader :uid

    private

    def delegate_and_call_slave_method(name, *args, &block)
      # Cache the delegated method, so we don't have to hit #method_missing
      # on every call.
      define_singleton_method(name) do |*inner_args, &inner_block|
        begin
          result = @slave.public_send(name, *inner_args, &inner_block)
          persist  # Update the slave data in database after each operation
          result
        rescue ArgumentError => exc
          raise Invalid, exc
        rescue Slave::Closed => exc
          raise Unavailable, exc
        end
      end

      # Now call the method, since that's our most common use case.
      public_send(name, *args, &block)
    end

    def method_missing(name, *args, &block)
      if @slave.respond_to?(name)
        delegate_and_call_slave_method(name, *args, &block)
      else
        super
      end
    end

    def respond_to_missing?(name, include_all = false)
      @slave.respond_to?(name) or super
    end
  end
end
