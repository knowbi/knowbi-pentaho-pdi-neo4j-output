package bi.know.kettle.neo4j.shared;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.neo4j.driver.Session;
import org.neo4j.kettle.core.Neo4jDefaults;
import org.neo4j.kettle.core.metastore.MetaStoreFactory;
import org.neo4j.kettle.shared.NeoConnection;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.metastore.api.IMetaStore;

import java.util.List;

public class NeoConnectionUtils {
  private static Class<?> PKG = NeoConnectionUtils.class; // for i18n purposes, needed by Translator2!!

  private static MetaStoreFactory<NeoConnection> staticFactory;

  public static MetaStoreFactory<NeoConnection> getConnectionFactory( IMetaStore metaStore ) {
    if ( staticFactory == null ) {
      staticFactory = new MetaStoreFactory<>( NeoConnection.class, metaStore, Neo4jDefaults.NAMESPACE );
    }
    return staticFactory;
  }

  public static NeoConnection newConnection( Shell shell, VariableSpace space, MetaStoreFactory<NeoConnection> factory ) {
    NeoConnection connection = new NeoConnection( space );
    boolean ok = false;
    while ( !ok ) {
      NeoConnectionDialog dialog = new NeoConnectionDialog( shell, connection );
      if ( dialog.open() ) {
        // write to metastore...
        try {
          if ( factory.loadElement( connection.getName() ) != null ) {
            MessageBox box = new MessageBox( shell, SWT.YES | SWT.NO | SWT.ICON_ERROR );
            box.setText( BaseMessages.getString( PKG, "NeoConnectionUtils.Error.ConnectionExists.Title" ) );
            box.setMessage( BaseMessages.getString( PKG, "NeoConnectionUtils.Error.ConnectionExists.Message" ) );
            int answer = box.open();
            if ( ( answer & SWT.YES ) != 0 ) {
              factory.saveElement( connection );
              ok = true;
            }
          } else {
            factory.saveElement( connection );
            ok = true;
          }
        } catch ( Exception exception ) {
          new ErrorDialog( shell,
            BaseMessages.getString( PKG, "NeoConnectionUtils.Error.ErrorSavingConnection.Title" ),
            BaseMessages.getString( PKG, "NeoConnectionUtils.Error.ErrorSavingConnection.Message" ),
            exception );
          return null;
        }
      } else {
        // Cancel
        return null;
      }
    }
    return connection;
  }

  public static void editConnection( Shell shell, VariableSpace space, MetaStoreFactory<NeoConnection> factory, String connectionName ) {
    if ( StringUtils.isEmpty( connectionName ) ) {
      return;
    }
    try {
      NeoConnection neoConnection = factory.loadElement( connectionName );
      neoConnection.initializeVariablesFrom( space );
      if ( neoConnection == null ) {
        newConnection( shell, space, factory );
      } else {
        NeoConnectionDialog neoConnectionDialog = new NeoConnectionDialog( shell, neoConnection );
        if ( neoConnectionDialog.open() ) {
          factory.saveElement( neoConnection );
        }
      }
    } catch ( Exception exception ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "NeoConnectionUtils.Error.ErrorEditingConnection.Title" ),
        BaseMessages.getString( PKG, "NeoConnectionUtils.Error.ErrorEditingConnection.Message" ),
        exception );
    }
  }

  public static void deleteConnection( Shell shell, MetaStoreFactory<NeoConnection> factory, String connectionName ) {
    if ( StringUtils.isEmpty( connectionName ) ) {
      return;
    }

    MessageBox box = new MessageBox( shell, SWT.YES | SWT.NO | SWT.ICON_ERROR );
    box.setText( BaseMessages.getString( PKG, "NeoConnectionUtils.DeleteConnectionConfirmation.Title" ) );
    box.setMessage( BaseMessages.getString( PKG, "NeoConnectionUtils.DeleteConnectionConfirmation.Message", connectionName ) );
    int answer = box.open();
    if ( ( answer & SWT.YES ) != 0 ) {
      try {
        factory.deleteElement( connectionName );
      } catch ( Exception exception ) {
        new ErrorDialog( shell,
          BaseMessages.getString( PKG, "NeoConnectionUtils.Error.ErrorDeletingConnection.Title" ),
          BaseMessages.getString( PKG, "NeoConnectionUtils.Error.ErrorDeletingConnection.Message", connectionName ),
          exception );
      }
    }
  }

  public static final void createNodeIndex( LogChannelInterface log, Session session, List<String> labels, List<String> keyProperties ) {

    // If we have no properties or labels, we have nothing to do here
    //
    if ( keyProperties.size() == 0 ) {
      return;
    }
    if ( labels.size() == 0 ) {
      return;
    }

    // We only use the first label for index or constraint
    //
    String labelsClause = ":" + labels.get( 0 );

    // CREATE CONSTRAINT ON (n:NodeLabel) ASSERT n.property1 IS UNIQUE
    //
    if ( keyProperties.size() == 1 ) {
      String property = keyProperties.get( 0 );
      String constraintCypher = "CREATE CONSTRAINT ON (n" + labelsClause + ") ASSERT n." + property + " IS UNIQUE;";

      log.logDetailed( "Creating constraint : " + constraintCypher );
      session.run( constraintCypher );

      // This creates an index, no need to go further here...
      //
      return;
    }

    // Composite index case...
    //
    // CREATE INDEX ON :NodeLabel(property, property2, ...);
    //
    String indexCypher = "CREATE INDEX ON ";

    indexCypher += labelsClause;
    indexCypher += "(";
    boolean firstProperty = true;
    for ( String property : keyProperties ) {
      if ( firstProperty ) {
        firstProperty = false;
      } else {
        indexCypher += ", ";
      }
      indexCypher += property;
    }
    indexCypher += ")";

    log.logDetailed( "Creating index : " + indexCypher );
    session.run( indexCypher );
  }

}
