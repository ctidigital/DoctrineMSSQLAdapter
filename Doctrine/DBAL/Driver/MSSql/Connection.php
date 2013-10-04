<?php

 /**
 * CTI Digital
 *
 * @author Jason Brown <j.brown@ctidigital.com>
 */

namespace Application\Database\Doctrine\DBAL\Driver\MSSql;

use Application\Database\Doctrine\DBAL\Driver\MSSql\Exception;

/**
 * Class Connection
 * @package Application\Database\Doctrine\DBAL\MSSql
 */
class Connection implements \Doctrine\DBAL\Driver\Connection
{
    /**
     * @var resource
     */
    protected $conn;

    /**
     * @param string $serverName
     * @param array  $connectionOptions
     */
    public function __construct($serverName, $connectionOptions)
    {
        $this->conn = mssql_connect($serverName, $connectionOptions['username'], $connectionOptions['password']);

        if ( ! $this->conn) {
            throw new Exception( mssql_get_last_message() );
        }
        $this->lastInsertId = new LastInsertId();

        // Check Database has been provided
        if(isset($connectionOptions['database']))
        {
            // Escape database name
            $databaseName = '[' . $connectionOptions['database'] . ']';

            // Select active Database
            mssql_select_db($databaseName, $this->conn);
        }else{
            throw new Exception('Database name has not been provided');
        }
    }

    /**
     * Prepares a statement for execution and returns a Statement object.
     *
     * @param string $prepareString
     *
     * @return \Doctrine\DBAL\Driver\Statement
     */
    function prepare($prepareString)
    {
        return new Statement($this->conn, $prepareString, $this->lastInsertId);
    }

    /**
     * Executes an SQL statement, returning a result set as a Statement object.
     *
     * @return \Doctrine\DBAL\Driver\Statement
     */
    function query()
    {
        $args = func_get_args();
        $sql = $args[0];
        $stmt = $this->prepare($sql);
        $stmt->execute();

        return $stmt;
    }

    /**
     * Quotes a string for use in a query.
     *
     * @param string $input
     * @param integer $type
     *
     * @return string
     */
    function quote($input, $type = \PDO::PARAM_STR)
    {
        if (is_int($input)) {
            return $input;
        } else if (is_float($input)) {
            return sprintf('%F', $input);
        }

        return "'" . str_replace("'", "''", $input) . "'";
    }

    /**
     * Executes an SQL statement and return the number of affected rows.
     *
     * @param string $statement
     *
     * @return integer
     */
    function exec($statement)
    {
        $stmt = $this->prepare($statement);
        $stmt->execute();

        return $stmt->rowCount();
    }

    /**
     * Returns the ID of the last inserted row or sequence value.
     *
     * @param string|null $name
     *
     * @return string
     */
    function lastInsertId($name = null)
    {
        if ($name !== null) {
            $sql = "SELECT IDENT_CURRENT(".$this->quote($name).") AS LastInsertId";
            $stmt = $this->prepare($sql);
            $stmt->execute();

            return $stmt->fetchColumn();
        }

        return $this->lastInsertId->getId();
    }

    /**
     * Initiates a transaction.
     *
     * @return boolean TRUE on success or FALSE on failure.
     */
    function beginTransaction()
    {
        /*if(!mssql_query("BEGIN TRANSACTION", $this->conn))
        {
            throw new Exception( mssql_get_last_message() );
        }*/
    }

    /**
     * Commits a transaction.
     *
     * @return boolean TRUE on success or FALSE on failure.
     */
    function commit()
    {
        /*if(!mssql_query("COMMIT", $this->conn))
        {
            throw new Exception( mssql_get_last_message() );
        }*/
    }

    /**
     * Rolls back the current transaction, as initiated by beginTransaction().
     *
     * @return boolean TRUE on success or FALSE on failure.
     */
    function rollBack()
    {
        /*if(!mssql_query("ROLLBACK", $this->conn))
        {
            throw new Exception( mssql_get_last_message() );
        }*/
    }

    /**
     * Returns the error code associated with the last operation on the database handle.
     *
     * @return string|null The error code, or null if no operation has been run on the database handle.
     */
    function errorCode()
    {
        // TODO: Implement errorCode() method.
    }

    /**
     * Returns extended error information associated with the last operation on the database handle.
     *
     * @return array
     */
    function errorInfo()
    {
        // TODO: Implement errorInfo() method.
    }
}