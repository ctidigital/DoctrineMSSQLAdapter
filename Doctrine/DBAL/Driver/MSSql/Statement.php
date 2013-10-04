<?php

 /**
 * CTI Digital
 *
 * @author Jason Brown <j.brown@ctidigital.com>
 */

namespace Application\Database\Doctrine\DBAL\Driver\MSSql;

use Boilerplate\Exception;
use Boilerplate\Log;
use PDO;
use Traversable;

/**
 * Class Statement
 * @package Application\Database\Doctrine\DBAL\Driver\MSSql
 */
class Statement implements \IteratorAggregate, \Doctrine\DBAL\Driver\Statement
{

    /**
     * The MSSQL Resource.
     *
     * @var resource
     */
    private $conn;

    /**
     * The SQL statement to execute.
     *
     * @var string
     */
    private $sql;

    /**
     * The MSSQL statement resource.
     *
     * @var resource
     */
    private $stmt;

    /**
     * Parameters to bind.
     *
     * @var array
     */
    private $params = array();

    /**
     * Translations.
     *
     * @var array
     */
    private static $fetchMap = array(
        PDO::FETCH_BOTH => MSSQL_BOTH,
        PDO::FETCH_ASSOC => MSSQL_ASSOC,
        PDO::FETCH_NUM => MSSQL_NUM,
    );

    /**
     * The fetch style.
     *
     * @param integer
     */
    private $defaultFetchMode = PDO::FETCH_BOTH;

    /**
     * The last insert ID.
     *
     * @var \Doctrine\DBAL\Driver\SQLSrv\LastInsertId|null
     */
    private $lastInsertId;

    /**
     * Append to any INSERT query to retrieve the last insert id.
     *
     * @var string
     */
    const LAST_INSERT_ID_SQL = ';SELECT SCOPE_IDENTITY() AS LastInsertId;';

    /**
     * @param resource     $conn
     * @param string       $sql
     * @param integer|null $lastInsertId
     */
    public function __construct($conn, $sql, $lastInsertId = null)
    {
        $this->conn = $conn;
        $this->sql = $sql;

        if (stripos($sql, 'INSERT INTO ') === 0) {
            $this->sql .= self::LAST_INSERT_ID_SQL; // @TODO determine best approach for IDENTITY
            $this->lastInsertId = $lastInsertId;
        }
    }

    /**
     * (PHP 5 &gt;= 5.0.0)<br/>
     * Retrieve an external iterator
     * @link http://php.net/manual/en/iteratoraggregate.getiterator.php
     * @return Traversable An instance of an object implementing <b>Iterator</b> or
     * <b>Traversable</b>
     */
    public function getIterator()
    {
        $data = $this->fetchAll();

        return new \ArrayIterator($data);
    }

    /**
     * Closes the cursor, enabling the statement to be executed again.
     *
     * @return boolean TRUE on success or FALSE on failure.
     */
    public function closeCursor()
    {
        if ($this->stmt) {
            //mssql_free_statement($this->stmt);
        }
    }

    /**
     * Returns the number of columns in the result set
     *
     * @return integer The number of columns in the result set represented
     *                 by the PDOStatement object. If there is no result set,
     *                 this method should return 0.
     */
    public function columnCount()
    {
        return  mssql_num_fields($this->stmt);
    }

    /**
     * Sets the fetch mode to use while iterating this statement.
     *
     * @param integer $fetchMode
     * @param mixed $arg2
     * @param mixed $arg3
     *
     * @return boolean
     */
    public function setFetchMode($fetchMode, $arg2 = null, $arg3 = null)
    {
        $this->defaultFetchMode = $fetchMode;

        return true;
    }

    /**
     * @see Query::HYDRATE_* constants
     *
     * @param integer|null $fetchMode Controls how the next row will be returned to the caller.
     *                                This value must be one of the Query::HYDRATE_* constants,
     *                                defaulting to Query::HYDRATE_BOTH
     *
     * @return mixed
     */
    public function fetch($fetchMode = null)
    {
        $fetchMode = $fetchMode ?: $this->defaultFetchMode;

        if (isset(self::$fetchMap[$fetchMode])) {
            return mssql_fetch_array($this->stmt);
        } else if ($fetchMode == PDO::FETCH_OBJ || $fetchMode == PDO::FETCH_CLASS) {
            $className = null;
            $ctorArgs = null;
            if (func_num_args() >= 2) {
                $args = func_get_args();
                $className = $args[1];
                $ctorArgs = (isset($args[2])) ? $args[2] : array();
            }
            return mssql_fetch_object($this->stmt, $className, $ctorArgs);
        }

        throw new Exception('Fetch mode is not supported!');
    }

    /**
     * Returns an array containing all of the result set rows.
     *
     * @param integer|null $fetchMode Controls how the next row will be returned to the caller.
     *                                This value must be one of the Query::HYDRATE_* constants,
     *                                defaulting to Query::HYDRATE_BOTH
     *
     * @return array
     */
    public function fetchAll($fetchMode = null)
    {
        $className = null;
        $ctorArgs = null;
        if (func_num_args() >= 2) {
            $args = func_get_args();
            $className = $args[1];
            $ctorArgs = (isset($args[2])) ? $args[2] : array();
        }

        $rows = array();
        while ($row = $this->fetch($fetchMode, $className, $ctorArgs)) {
            $rows[] = $row;
        }

        return $rows;
    }

    /**
     * Returns a single column from the next row of a result set or FALSE if there are no more rows.
     *
     * @param integer $columnIndex 0-indexed number of the column you wish to retrieve from the row.
     *                             If no value is supplied, PDOStatement->fetchColumn()
     *                             fetches the first column.
     *
     * @return string|boolean A single column in the next row of a result set, or FALSE if there are no more rows.
     */
    public function fetchColumn($columnIndex = 0)
    {
        $row = $this->fetch(self::$fetchMap[PDO::FETCH_NUM]);

        return $row[$columnIndex];
    }

    /**
     * Binds a value to a corresponding named or positional
     * placeholder in the SQL statement that was used to prepare the statement.
     *
     * @param mixed $param Parameter identifier. For a prepared statement using named placeholders,
     *                       this will be a parameter name of the form :name. For a prepared statement
     *                       using question mark placeholders, this will be the 1-indexed position of the parameter.
     * @param mixed $value The value to bind to the parameter.
     * @param integer $type  Explicit data type for the parameter using the PDO::PARAM_* constants.
     *
     * @return boolean TRUE on success or FALSE on failure.
     */
    function bindValue($param, $value, $type = null)
    {
        return $this->bindParam($param, $value, $type,null);
    }

    /**
     * Binds a PHP variable to a corresponding named or question mark placeholder in the
     * SQL statement that was use to prepare the statement. Unlike PDOStatement->bindValue(),
     * the variable is bound as a reference and will only be evaluated at the time
     * that PDOStatement->execute() is called.
     *
     * Most parameters are input parameters, that is, parameters that are
     * used in a read-only fashion to build up the query. Some drivers support the invocation
     * of stored procedures that return data as output parameters, and some also as input/output
     * parameters that both send in data and are updated to receive it.
     *
     * @param mixed $column   Parameter identifier. For a prepared statement using named placeholders,
     *                               this will be a parameter name of the form :name. For a prepared statement using
     *                               question mark placeholders, this will be the 1-indexed position of the parameter.
     * @param mixed $variable Name of the PHP variable to bind to the SQL statement parameter.
     * @param integer|null $type     Explicit data type for the parameter using the PDO::PARAM_* constants. To return
     *                               an INOUT parameter from a stored procedure, use the bitwise OR operator to set the
     *                               PDO::PARAM_INPUT_OUTPUT bits for the data_type parameter.
     * @param integer|null $length   You must specify maxlength when using an OUT bind
     *                               so that PHP allocates enough memory to hold the returned value.
     *
     * @return boolean TRUE on success or FALSE on failure.
     */
    function bindParam($column, &$variable, $type = null, $length = null)
    {
        if (!is_numeric($column)) {
            //throw new SQLSrvException("sqlsrv does not support named parameters to queries, use question mark (?) placeholders instead.");
        }

        if ($type === \PDO::PARAM_LOB) {
            //$this->params[$column-1] = array($variable, SQLSRV_PARAM_IN, SQLSRV_PHPTYPE_STREAM(SQLSRV_ENC_BINARY), SQLSRV_SQLTYPE_VARBINARY('max'));
            throw new Exception('Trying to store LOB');
        } else {
            $this->params[$column-1] = $variable;
        }
    }

    /**
     * Fetches the SQLSTATE associated with the last operation on the statement handle.
     *
     * @see Doctrine_Adapter_Interface::errorCode()
     *
     * @return string The error code string.
     */
    function errorCode()
    {
        return mssql_get_last_message();
    }

    /**
     * Fetches extended error information associated with the last operation on the statement handle.
     *
     * @see Doctrine_Adapter_Interface::errorInfo()
     *
     * @return array The error info array.
     */
    function errorInfo()
    {
        return mssql_get_last_message();
    }

    /**
     * Executes a prepared statement
     *
     * If the prepared statement included parameter markers, you must either:
     * call PDOStatement->bindParam() to bind PHP variables to the parameter markers:
     * bound variables pass their value as input and receive the output value,
     * if any, of their associated parameter markers or pass an array of input-only
     * parameter values.
     *
     *
     * @param array|null $params An array of values with as many elements as there are
     *                           bound parameters in the SQL statement being executed.
     *
     * @return boolean TRUE on success or FALSE on failure.
     */
    function execute($params = null)
    {
        if ($params) {
            $hasZeroIndex = array_key_exists(0, $params);
            foreach ($params as $key => $val) {
                $key = ($hasZeroIndex && is_numeric($key)) ? $key + 1 : $key;
                $this->bindValue($key, $val);
            }
        }

        foreach($this->params as $param)
        {
            $this->sql = substr_replace($this->sql, "'" . $param . "'", strpos($this->sql, '?'), 1);
        }

        $this->stmt = mssql_query($this->sql, $this->conn);

        if ( ! $this->stmt) {
            throw new Exception($this->errorInfo());
        }

        if ($this->lastInsertId) {
            mssql_next_result($this->stmt);
           $this->lastInsertId->setId( mssql_field_seek($this->stmt, 0) );
        }
    }

    /**
     * Returns the number of rows affected by the last DELETE, INSERT, or UPDATE statement
     * executed by the corresponding object.
     *
     * If the last SQL statement executed by the associated Statement object was a SELECT statement,
     * some databases may return the number of rows returned by that statement. However,
     * this behaviour is not guaranteed for all databases and should not be
     * relied on for portable applications.
     *
     * @return integer The number of rows.
     */
    function rowCount()
    {
        return  mssql_rows_affected($this->conn);
    }
}